/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.generator.values.internal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.Internal;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValueBasicResultBuilder;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.generator.values.internal.GeneratedValuesHelper;
import org.hibernate.generator.values.internal.GeneratedValuesImpl;
import org.hibernate.generator.values.internal.GeneratedValuesMappingProducer;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.id.insert.GetGeneratedKeysDelegate;
import org.hibernate.id.insert.UniqueKeySelectingDelegate;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.id.insert.ReactiveInsertReturningDelegate;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.reactive.sql.results.internal.ReactiveDirectResultSetAccess;
import org.hibernate.reactive.sql.results.internal.ReactiveResultsHelper;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.results.internal.RowTransformerArrayImpl;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.type.descriptor.WrapperOptions;

import static org.hibernate.generator.internal.NaturalIdHelper.getNaturalIdPropertyNames;
import static org.hibernate.generator.values.internal.GeneratedValuesHelper.noCustomSql;
import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.NONE;

/**
 * @see org.hibernate.generator.values.internal.GeneratedValuesHelper
 */
@Internal
public class ReactiveGeneratedValuesHelper {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( IdentifierGeneratorHelper.class );

	/**
	 *
	 * @see GeneratedValuesHelper#getGeneratedValuesDelegate(EntityPersister, EventType)
	 */
	public static GeneratedValuesMutationDelegate getGeneratedValuesDelegate(
			EntityPersister persister,
			EventType timing) {
		final boolean hasGeneratedProperties = !persister.getGeneratedProperties( timing ).isEmpty();
		final boolean hasRowId = timing == EventType.INSERT && persister.getRowIdMapping() != null;
		final Dialect dialect = persister.getFactory().getJdbcServices().getDialect();

		if ( hasRowId && dialect.supportsInsertReturning() && dialect.supportsInsertReturningRowId()
				&& noCustomSql( persister, timing ) ) {
			// Special case for RowId on INSERT, since GetGeneratedKeysDelegate doesn't support it
			// make InsertReturningDelegate the preferred method if the dialect supports it
			return new ReactiveInsertReturningDelegate( persister, timing );
		}

		if ( !hasGeneratedProperties ) {
			return null;
		}

		if ( dialect.supportsInsertReturningGeneratedKeys()
				&& persister.getFactory().getSessionFactoryOptions().isGetGeneratedKeysEnabled() ) {
			return new GetGeneratedKeysDelegate( persister, false, timing );
		}
		else if ( supportsReturning( dialect, timing ) && noCustomSql( persister, timing ) ) {
			return new ReactiveInsertReturningDelegate( persister, timing );
		}
		else if ( timing == EventType.INSERT && persister.getNaturalIdentifierProperties() != null
				&& !persister.getEntityMetamodel().isNaturalIdentifierInsertGenerated() ) {
			return new UniqueKeySelectingDelegate(
					persister,
					getNaturalIdPropertyNames( persister ),
					timing
			);
		}
		return null;
	}

	private static boolean supportsReturning(Dialect dialect, EventType timing) {
		return timing == EventType.INSERT ? dialect.supportsInsertReturning() : dialect.supportsUpdateReturning();
	}

	/**
	 * Reads the {@link EntityPersister#getGeneratedProperties(EventType) generated values}
	 * for the specified {@link ResultSet}.
	 *
	 * @param resultSet The result set from which to extract the generated values
	 * @param persister The entity type which we're reading the generated values for
	 * @param wrapperOptions The session
	 *
	 * @return The generated values
	 *
	 * @throws HibernateException Indicates a problem reading back a generated value
	 */
	public static CompletionStage<GeneratedValues> getGeneratedValues(
			ResultSet resultSet,
			EntityPersister persister,
			EventType timing,
			WrapperOptions wrapperOptions) {
		if ( resultSet == null ) {
			return null;
		}

		final GeneratedValuesMutationDelegate delegate = persister.getMutationDelegate(
				timing == EventType.INSERT ? MutationType.INSERT : MutationType.UPDATE
		);
		final GeneratedValuesMappingProducer mappingProducer =
				(GeneratedValuesMappingProducer) delegate.getGeneratedValuesMappingProducer();
		final List<GeneratedValueBasicResultBuilder> resultBuilders = mappingProducer.getResultBuilders();
		final List<ModelPart> generatedProperties = new ArrayList<>( resultBuilders.size() );
		for ( GeneratedValueBasicResultBuilder resultBuilder : resultBuilders ) {
			generatedProperties.add( resultBuilder.getModelPart() );
		}

		final GeneratedValuesImpl generatedValues = new GeneratedValuesImpl( generatedProperties );
		return readGeneratedValues( resultSet, persister, mappingProducer, wrapperOptions.getSession() )
				.thenApply( results -> {
					if ( LOG.isDebugEnabled() ) {
						LOG.debugf( "Extracted generated values %s: %s", MessageHelper.infoString( persister ), results );
					}

					for ( int i = 0; i < results.length; i++ ) {
						generatedValues.addGeneratedValue( generatedProperties.get( i ), results[i] );
					}

					return generatedValues;
				} );
	}

	private static CompletionStage<Object[]> readGeneratedValues(
			ResultSet resultSet,
			EntityPersister persister,
			JdbcValuesMappingProducer mappingProducer,
			SharedSessionContractImplementor session) {
		final ExecutionContext executionContext = new BaseExecutionContext( session );


		final ReactiveDirectResultSetAccess directResultSetAccess;
		try {
			directResultSetAccess = new ReactiveDirectResultSetAccess( session, (PreparedStatement) resultSet.getStatement(), resultSet );
		}
		catch (SQLException e) {
			throw new HibernateException( "Could not retrieve statement from generated values result set", e );
		}

		final ReactiveValuesResultSet jdbcValues = new ReactiveValuesResultSet(
				directResultSetAccess,
				null,
				null,
				QueryOptions.NONE,
				true,
				mappingProducer.resolve(
						directResultSetAccess,
						session.getLoadQueryInfluencers(),
						session.getSessionFactory()
				),
				null,
				executionContext
		);

		final JdbcValuesSourceProcessingOptions processingOptions = new JdbcValuesSourceProcessingOptions() {
			@Override
			public Object getEffectiveOptionalObject() {
				return null;
			}

			@Override
			public String getEffectiveOptionalEntityName() {
				return null;
			}

			@Override
			public Object getEffectiveOptionalId() {
				return null;
			}

			@Override
			public boolean shouldReturnProxies() {
				return true;
			}
		};

		final JdbcValuesSourceProcessingStateStandardImpl valuesProcessingState = new JdbcValuesSourceProcessingStateStandardImpl(
				executionContext,
				processingOptions
		);

		final ReactiveRowReader<Object[]> rowReader = ReactiveResultsHelper.createRowReader(
				session.getSessionFactory(),
				RowTransformerArrayImpl.instance(),
				Object[].class,
				jdbcValues
		);

		final ReactiveRowProcessingState rowProcessingState = new ReactiveRowProcessingState( valuesProcessingState, executionContext, rowReader, jdbcValues );
		return ReactiveListResultsConsumer.<Object[]>instance( NONE )
				.consume( jdbcValues, session, processingOptions, valuesProcessingState, rowProcessingState, rowReader )
				.thenApply( results -> {
					if ( results.isEmpty() ) {
						throw new HibernateException( "The database returned no natively generated values : " + persister.getNavigableRole().getFullPath() );
					}
					return results.get( 0 );
				} );
	}
}

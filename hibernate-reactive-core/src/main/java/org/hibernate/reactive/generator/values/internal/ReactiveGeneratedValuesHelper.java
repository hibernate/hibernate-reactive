/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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
import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.Generator;
import org.hibernate.generator.GeneratorCreationContext;
import org.hibernate.generator.values.GeneratedValueBasicResultBuilder;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.generator.values.internal.GeneratedValuesHelper;
import org.hibernate.generator.values.internal.GeneratedValuesImpl;
import org.hibernate.generator.values.internal.GeneratedValuesMappingProducer;
import org.hibernate.id.CompositeNestedGeneratedValueGenerator;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.SelectGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.EmulatedSequenceReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.ReactiveCompositeNestedGeneratedValueGenerator;
import org.hibernate.reactive.id.impl.ReactiveGeneratorWrapper;
import org.hibernate.reactive.id.impl.ReactiveSequenceIdentifierGenerator;
import org.hibernate.reactive.id.impl.TableReactiveIdentifierGenerator;
import org.hibernate.reactive.id.insert.ReactiveGetGeneratedKeysDelegate;
import org.hibernate.reactive.id.insert.ReactiveInsertReturningDelegate;
import org.hibernate.reactive.id.insert.ReactiveUniqueKeySelectingDelegate;
import org.hibernate.reactive.logging.impl.Log;
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
import org.hibernate.type.descriptor.WrapperOptions;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.generator.values.internal.GeneratedValuesHelper.noCustomSql;
import static org.hibernate.internal.NaturalIdHelper.getNaturalIdPropertyNames;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.NONE;
import static org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions.NO_OPTIONS;

/**
 * @see org.hibernate.generator.values.internal.GeneratedValuesHelper
 */
@Internal
public class ReactiveGeneratedValuesHelper {
	private static final Log LOG = make( Log.class, lookup() );

	/**
	 *
	 * @see GeneratedValuesHelper#getGeneratedValuesDelegate(EntityPersister, EventType)
	 */
	public static GeneratedValuesMutationDelegate getGeneratedValuesDelegate(EntityPersister persister, EventType timing) {
		final List<? extends ModelPart> generatedProperties = persister.getGeneratedProperties( timing );
		final boolean hasGeneratedProperties = !generatedProperties.isEmpty();
		final boolean hasRowId = timing == EventType.INSERT && persister.getRowIdMapping() != null;
		final Dialect dialect = persister.getFactory().getJdbcServices().getDialect();

		final boolean hasFormula =
				generatedProperties.stream()
						.anyMatch( part -> part instanceof SelectableMapping selectable
								&& selectable.isFormula() );

		// Cockroach supports insert returning it but the CockroachDb#supportsInsertReturningRowId() wrongly returns false ( https://hibernate.atlassian.net/browse/HHH-19717 )
		boolean supportsInsertReturningRowId = dialect.supportsInsertReturningRowId() || dialect instanceof CockroachDialect;
		if ( hasRowId
				&& supportsInsertReturning( dialect )
				&& supportsInsertReturningRowId
				&& noCustomSql( persister, timing ) ) {
			// Special case for RowId on INSERT, since GetGeneratedKeysDelegate doesn't support it
			// make InsertReturningDelegate the preferred method if the dialect supports it
			return new ReactiveInsertReturningDelegate( persister, timing );
		}

		if ( !hasGeneratedProperties ) {
			return null;
		}

		if ( supportsReturning( dialect, timing ) && noCustomSql( persister, timing ) ) {
			return new ReactiveInsertReturningDelegate( persister, timing );
		}
		else if ( !hasFormula && dialect.supportsInsertReturningGeneratedKeys() ) {
			return new ReactiveGetGeneratedKeysDelegate( persister, false, timing );
		}
		else if ( timing == EventType.INSERT && persister.getNaturalIdentifierProperties() != null && !persister.getEntityMetamodel()
				.isNaturalIdentifierInsertGenerated() ) {
			return new ReactiveUniqueKeySelectingDelegate( persister, getNaturalIdPropertyNames( persister ), timing );
		}
		return null;
	}

	public static boolean supportReactiveGetGeneratedKey(Dialect dialect, List<? extends ModelPart> generatedProperties) {
		return dialect instanceof OracleDialect
				|| (dialect instanceof MySQLDialect && generatedProperties.size() == 1 && !(dialect instanceof MariaDBDialect));
	}

	public static boolean supportsReturning(Dialect dialect, EventType timing) {
		if ( dialect instanceof CockroachDialect ) {
			// Cockroach supports insert and update returning but the CockroachDb#supportsInsertReturning() wrongly returns false ( https://hibernate.atlassian.net/browse/HHH-19717 )
			return true;
		}
		return timing == EventType.INSERT
				? dialect.supportsInsertReturning()
				: dialect.supportsUpdateReturning();
	}

	public static boolean supportsInsertReturning(Dialect dialect) {
		if ( dialect instanceof CockroachDialect ) {
			// Cockroach supports insert returning but the CockroachDb#supportsInsertReturning() wrongly returns false ( https://hibernate.atlassian.net/browse/HHH-19717 )
			return true;
		}
		return dialect.supportsInsertReturning();
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

		final JdbcValuesSourceProcessingStateStandardImpl valuesProcessingState = new JdbcValuesSourceProcessingStateStandardImpl(
				executionContext,
				NO_OPTIONS
		);

		final ReactiveRowReader<Object[]> rowReader = ReactiveResultsHelper.createRowReader(
				session.getSessionFactory(),
				RowTransformerArrayImpl.instance(),
				Object[].class,
				jdbcValues
		);

		final ReactiveRowProcessingState rowProcessingState = new ReactiveRowProcessingState( valuesProcessingState, executionContext, rowReader, jdbcValues );
		return ReactiveListResultsConsumer.<Object[]>instance( NONE )
				.consume( jdbcValues, session, NO_OPTIONS, valuesProcessingState, rowProcessingState, rowReader )
				.thenApply( results -> {
					if ( results.isEmpty() ) {
						throw new HibernateException( "The database returned no natively generated values : " + persister.getNavigableRole().getFullPath() );
					}
					return results.get( 0 );
				} );
	}

	public static Generator augmentWithReactiveGenerator(
			Generator generator,
			GeneratorCreationContext creationContext,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		if ( generator instanceof SequenceStyleGenerator sequenceStyleGenerator) {
			final DatabaseStructure structure = sequenceStyleGenerator.getDatabaseStructure();
			if ( structure instanceof TableStructure ) {
				return initialize( (IdentifierGenerator) generator, new EmulatedSequenceReactiveIdentifierGenerator( (TableStructure) structure, runtimeModelCreationContext ), creationContext );
			}
			if ( structure instanceof SequenceStructure ) {
				return initialize( (IdentifierGenerator) generator, new ReactiveSequenceIdentifierGenerator( structure, runtimeModelCreationContext ), creationContext );
			}
			throw LOG.unknownStructureType();
		}
		if ( generator instanceof TableGenerator tableGenerator ) {
			return initialize(
					(IdentifierGenerator) generator,
					new TableReactiveIdentifierGenerator( tableGenerator, runtimeModelCreationContext ),
					creationContext
			);
		}
		if ( generator instanceof SelectGenerator ) {
			throw LOG.selectGeneratorIsNotSupportedInHibernateReactive();
		}
		if ( generator instanceof CompositeNestedGeneratedValueGenerator compositeNestedGeneratedValueGenerator ) {
			final ReactiveCompositeNestedGeneratedValueGenerator reactiveCompositeNestedGeneratedValueGenerator = new ReactiveCompositeNestedGeneratedValueGenerator(
					compositeNestedGeneratedValueGenerator,
					creationContext,
					runtimeModelCreationContext
			);
			return initialize(
					(IdentifierGenerator) generator,
					reactiveCompositeNestedGeneratedValueGenerator,
					creationContext
			);
		}
		//nothing to do
		return generator;
	}

	private static Generator initialize(
			IdentifierGenerator idGenerator,
			ReactiveIdentifierGenerator<?> reactiveIdGenerator,
			GeneratorCreationContext creationContext) {
		( (Configurable) reactiveIdGenerator ).initialize( creationContext.getSqlStringGenerationContext() );
		return new ReactiveGeneratorWrapper( reactiveIdGenerator, idGenerator );
	}

}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValueBasicResultBuilder;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.internal.TableUpdateReturningBuilder;
import org.hibernate.id.insert.AbstractReturningDelegate;
import org.hibernate.id.insert.InsertReturningDelegate;
import org.hibernate.id.insert.TableInsertReturningBuilder;
import org.hibernate.jdbc.Expectation;
import org.hibernate.metamodel.mapping.BasicEntityIdentifierMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.model.ast.MutatingTableReference;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;

import static java.sql.Statement.NO_GENERATED_KEYS;
import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.generator.values.internal.GeneratedValuesHelper.getActualGeneratedModelPart;
import static org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper.getGeneratedValues;

/**
 * @see InsertReturningDelegate
 */
public class ReactiveInsertReturningDelegate extends AbstractReturningDelegate implements ReactiveAbstractReturningDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final EntityPersister persister;
	private final MutatingTableReference tableReference;
	private final List<ColumnReference> generatedColumns;

	public ReactiveInsertReturningDelegate(EntityPersister persister, EventType timing) {
		this( persister, timing, false );
	}

	public ReactiveInsertReturningDelegate(EntityPersister persister, Dialect dialect) {
		// With JDBC it's possible to enabled GetGeneratedKeys for identity generation.
		// Vert.x doesn't have this option, so we always use the same strategy for all database.
		// But MySQL requires setting supportsArbitraryValues to false or it's not going to work.
		this( persister, INSERT, supportsArbitraryValues( dialect ) );
	}

	private static boolean supportsArbitraryValues( Dialect dialect) {
		return !( dialect instanceof MySQLDialect );
	}

	private ReactiveInsertReturningDelegate(EntityPersister persister, EventType timing, boolean supportsArbitraryValues) {
		super(
				persister,
				timing,
				supportsArbitraryValues,
				persister.getFactory().getJdbcServices().getDialect().supportsInsertReturningRowId()
		);
		this.persister = persister;
		this.tableReference = new MutatingTableReference( persister.getIdentifierTableMapping() );
		final List<GeneratedValueBasicResultBuilder> resultBuilders = jdbcValuesMappingProducer.getResultBuilders();
		this.generatedColumns = new ArrayList<>( resultBuilders.size() );
		for ( GeneratedValueBasicResultBuilder resultBuilder : resultBuilders ) {
			generatedColumns.add( new ColumnReference(
					tableReference,
					getActualGeneratedModelPart( resultBuilder.getModelPart() )
			) );
		}
	}
	@Override
	public TableMutationBuilder<?> createTableMutationBuilder(
			Expectation expectation,
			SessionFactoryImplementor sessionFactory) {
		if ( getTiming() == INSERT ) {
			return new TableInsertReturningBuilder( persister, tableReference, generatedColumns, sessionFactory );
		}
		else {
			return new TableUpdateReturningBuilder( persister, tableReference, generatedColumns, sessionFactory );
		}
	}

	@Override
	public String prepareIdentifierGeneratingInsert(String insertSQL) {
		return dialect().getIdentityColumnSupport().appendIdentitySelectToInsert(
				( (BasicEntityIdentifierMapping) persister.getRootEntityDescriptor().getIdentifierMapping() ).getSelectionExpression(),
				insertSQL
		);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, SharedSessionContractImplementor session) {
		return session.getJdbcCoordinator().getMutationStatementPreparer().prepareStatement( sql, NO_GENERATED_KEYS );
	}

	@Override
	public EntityPersister getPersister() {
		return persister;
	}

	@Override
	public GeneratedValues performMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactivePerformMutation" );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveExecuteAndExtractReturning(String sql, Object[] params, SharedSessionContractImplementor session)  {
		final Class<?> idType = getPersister().getIdentifierType().getReturnedClass();
		final String identifierColumnName = getPersister().getIdentifierColumnNames()[0];
		return ( (ReactiveConnectionSupplier) session )
				.getReactiveConnection()
				.insertAndSelectIdentifierAsResultSet( sql, params, idType, identifierColumnName )
				.thenCompose( rs -> getGeneratedValues( rs, getPersister(), getTiming(), session ) )
				.thenApply( this::validateGeneratedIdentityId );
	}

	@Override
	protected GeneratedValues executeAndExtractReturning(String sql, PreparedStatement preparedStatement, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteAndExtractReturning" );
	}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.AbstractReturningDelegate;
import org.hibernate.id.insert.Binder;
import org.hibernate.metamodel.mapping.BasicValuedModelPart;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.model.ast.MutatingTableReference;
import org.hibernate.type.Type;

import static org.hibernate.generator.values.internal.GeneratedValuesHelper.getActualGeneratedModelPart;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public abstract class ReactiveAbstractReturningDelegate extends AbstractReturningDelegate implements ReactiveInsertGeneratedIdentifierDelegate {
	private static final Log LOG = make( Log.class, MethodHandles.lookup() );

	private final EntityPersister persister;
	private final List<ColumnReference> generatedColumns;
	private final List<Class<?>> generatedValueTypes;

	public ReactiveAbstractReturningDelegate(
			EntityPersister persister,
			EventType timing,
			boolean supportsArbitraryValues,
			boolean supportsRowId) {
		super( persister, timing, supportsArbitraryValues, supportsRowId );
		this.persister = persister;
		final var resultBuilders = jdbcValuesMappingProducer.getResultBuilders();
		final MutatingTableReference tableReference = new MutatingTableReference( persister.getIdentifierTableMapping() );
		generatedColumns = new ArrayList<>( resultBuilders.size() );
		this.generatedValueTypes = new ArrayList<>( resultBuilders.size() );

		for ( var resultBuilder : resultBuilders ) {
			final BasicValuedModelPart modelPart = resultBuilder.getModelPart();
			final ColumnReference column = new ColumnReference( tableReference, getActualGeneratedModelPart( modelPart ) );
			generatedColumns.add( column );
			generatedValueTypes.add( modelPart.getJavaType().getJavaTypeClass() );
		}
	}

	@Override
	public CompletionStage<GeneratedValues> reactivePerformInsertReturning(String sql, SharedSessionContractImplementor session, Binder binder) {
		final String insertSql = createSqlString( sql, getGeneratedColumnNames(), session.getJdbcServices().getDialect() );
		final Object[] params = PreparedStatementAdaptor.bind( binder::bindValues );
		return reactiveExecuteAndExtractReturning( insertSql, params, session );
	}

	public CompletionStage<GeneratedValues> reactiveExecuteAndExtractReturning(String sql, Object[] params, SharedSessionContractImplementor session)  {
		return ( (ReactiveConnectionSupplier) session )
				.getReactiveConnection()
				.executeAndSelectGeneratedValues( sql, params, getGeneratedValueTypes(), getGeneratedColumnNames() )
				.thenCompose( rs -> ReactiveGeneratedValuesHelper.getGeneratedValues( rs, persister, getTiming(), session ) )
				.thenApply( this::validateGeneratedIdentityId );
	}

	@Override
	public CompletionStage<GeneratedValues> reactivePerformMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		final JdbcServices jdbcServices = session.getJdbcServices();
		final Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, jdbcServices );
			valueBindings.beforeStatement( details );
		} );

		final String sql = createSqlString( statementDetails.getSqlString(), getGeneratedColumnNames(), jdbcServices.getDialect() );
		return reactiveExecuteAndExtractReturning( sql, params, session )
				.whenComplete( (values, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( statementDetails.getMutatingTableDetails() );
				} );
	}

	public GeneratedValues validateGeneratedIdentityId(GeneratedValues generatedValues) {
		if ( generatedValues == null ) {
			throw LOG.noNativelyGeneratedValueReturned();
		}

		// CockroachDB might generate an identifier that fits an integer (and maybe a short) from time to time.
		// Users should not rely on it, or they might have random, hard to debug failures.
		Type identifierType = persister.getIdentifierType();
		if ( ( identifierType.getReturnedClass().equals( Short.class ) || identifierType.getReturnedClass().equals( Integer.class ) )
				&& persister.getFactory().getJdbcServices().getDialect() instanceof CockroachDialect ) {
			throw LOG.invalidIdentifierTypeForCockroachDB( identifierType.getReturnedClass(), persister.getEntityName() );
		}
		return generatedValues;
	}

	public String createSqlString(String sql, List<String> columnNames, Dialect dialect){
		return sql;
	}

	private List<String> getGeneratedColumnNames(){
		final List<ColumnReference> generatedColumns = getGeneratedColumns();
		final List<String> generatedColumnNames = new ArrayList<>(generatedColumns.size());
		generatedColumns.forEach( column -> generatedColumnNames.add( column.getColumnExpression() ) );
		return generatedColumnNames;
	}

	protected List<ColumnReference> getGeneratedColumns(){
		return generatedColumns;
	}

	protected List<Class<?>> getGeneratedValueTypes(){
		return generatedValueTypes;
	}

	protected EntityPersister getPersister(){
		return persister;
	}
}

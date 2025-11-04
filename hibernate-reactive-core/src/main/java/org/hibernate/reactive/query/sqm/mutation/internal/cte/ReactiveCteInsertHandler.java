/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.function.array.DdlTypeHelper;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.cte.CteInsertHandler;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.cte.CteTable;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcLockStrategy;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBinding;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;
import org.hibernate.sql.exec.spi.StatementAccess;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;
import org.hibernate.type.spi.TypeConfiguration;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class ReactiveCteInsertHandler extends CteInsertHandler implements ReactiveHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final Dialect dialect;

	public ReactiveCteInsertHandler(
			CteTable cteTable,
			SqmInsertStatement<?> sqmStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super( cteTable, sqmStatement, domainParameterXref, context, firstJdbcParameterBindingsConsumer );
		this.dialect = context.getSession().getDialect();
	}

	@Override
	public int execute(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(
			JdbcParameterBindings jdbcParameterBindings,
			DomainQueryExecutionContext context) {
		JdbcSelect jdbcSelect;

		if ( dialect instanceof PostgreSQLDialect ) {
			// need to replace parameters with explicit casts see https://github.com/eclipse-vertx/vertx-sql-client/issues/1540
			jdbcSelect = new PostgreSQLCteMutationSelect( getSelect(), jdbcParameterBindings, context );
		}
		else {
			jdbcSelect = getSelect();
		}

		return ( (ReactiveSharedSessionContractImplementor) context.getSession() )
				.reactiveAutoFlushIfRequired( jdbcSelect.getAffectedTableNames() )
				.thenCompose( v -> StandardReactiveSelectExecutor.INSTANCE
						.list(
								jdbcSelect,
								jdbcParameterBindings,
								SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context ),
								row -> row[0],
								null,
								ReactiveListResultsConsumer.UniqueSemantic.NONE,
								1
						)
						.thenApply( list -> ( (Number) list.get( 0 ) ).intValue() )
				);
	}

	/*
	 * A JdbcSelect wrapper that adds explicit type casts to parameters in the original SQL Select string.
	 * This is needed for PostgreSQL when using CTEs for mutation statements,
	 * See https://github.com/eclipse-vertx/vertx-sql-client/issues/1540 .
	 */
	public static class PostgreSQLCteMutationSelect implements JdbcSelect {
		private final JdbcSelect delegate;
		private final String sqlString;

		public PostgreSQLCteMutationSelect(
				JdbcSelect delegate,
				JdbcParameterBindings jdbcParameterBindings,
				DomainQueryExecutionContext context) {
			this.delegate = delegate;
			this.sqlString = getSqlStringWithExplicitParameterCasting( delegate, jdbcParameterBindings, context );
		}

		private static String getSqlStringWithExplicitParameterCasting(
				JdbcSelect original,
				JdbcParameterBindings jdbcParameterBindings,
				DomainQueryExecutionContext context) {
			final StringBuilder newSelect = new StringBuilder( original.getSqlString() );
			addExplicitCastToParameters(
					jdbcParameterBindings,
					newSelect,
					context.getSession().getSessionFactory().getMappingMetamodel().getTypeConfiguration()
			);
			return newSelect.toString();
		}

		private static void addExplicitCastToParameters(
				JdbcParameterBindings jdbcParameterBindings,
				StringBuilder newSelect,
				TypeConfiguration typeConfiguration) {
			jdbcParameterBindings.visitBindings(
					(jdbcParameter, jdbcParameterBinding) ->
							addExplicitCastToParameter(
									newSelect,
									typeConfiguration,
									jdbcParameter,
									jdbcParameterBinding
							)
			);
		}

		private static void addExplicitCastToParameter(
				StringBuilder newSelect,
				TypeConfiguration typeConfiguration,
				JdbcParameter jdbcParameter,
				JdbcParameterBinding jdbcParameterBinding) {
			final int index = jdbcParameter.getParameterId() + 1;
			final String parameterToReplace = "$" + index;
			final int start = newSelect.indexOf( parameterToReplace );
			newSelect.replace(
					start,
					start + parameterToReplace.length(),
					parameterToReplace + "::" + DdlTypeHelper.getCastTypeName(
							jdbcParameterBinding.getBindType(),
							typeConfiguration
					)
			);
		}

		@Override
		public JdbcValuesMappingProducer getJdbcValuesMappingProducer() {
			return delegate.getJdbcValuesMappingProducer();
		}

		@Override
		public JdbcLockStrategy getLockStrategy() {
			return delegate.getLockStrategy();
		}

		@Override
		public boolean usesLimitParameters() {
			return delegate.usesLimitParameters();
		}

		@Override
		public JdbcParameter getLimitParameter() {
			return delegate.getLimitParameter();
		}

		@Override
		public int getRowsToSkip() {
			return delegate.getRowsToSkip();
		}

		@Override
		public int getMaxRows() {
			return delegate.getMaxRows();
		}

		@Override
		public LoadedValuesCollector getLoadedValuesCollector() {
			return delegate.getLoadedValuesCollector();
		}

		@Override
		public void performPreActions(
				StatementAccess jdbcStatementAccess,
				Connection jdbcConnection,
				ExecutionContext executionContext) {
			delegate.performPreActions( jdbcStatementAccess, jdbcConnection, executionContext );
		}

		@Override
		public void performPostAction(
				boolean succeeded,
				StatementAccess jdbcStatementAccess,
				Connection jdbcConnection,
				ExecutionContext executionContext) {
			delegate.performPostAction( succeeded, jdbcStatementAccess, jdbcConnection, executionContext );
		}

		@Override
		public boolean dependsOnParameterBindings() {
			return delegate.dependsOnParameterBindings();
		}

		@Override
		public boolean isCompatibleWith(JdbcParameterBindings jdbcParameterBindings, QueryOptions queryOptions) {
			return delegate.isCompatibleWith( jdbcParameterBindings, queryOptions );
		}

		@Override
		public Set<String> getAffectedTableNames() {
			return delegate.getAffectedTableNames();
		}

		@Override
		public String getSqlString() {
			return sqlString;
		}

		@Override
		public List<JdbcParameterBinder> getParameterBinders() {
			return delegate.getParameterBinders();
		}
	}
}

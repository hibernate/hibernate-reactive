/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.sql.exec.spi.ReactiveJdbcMutationExecutor;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.model.MutationOperationGroup;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard
 */
public class ReactiveMutationExecutorStandard implements ReactiveJdbcMutationExecutor, MutationExecutorService {

	public static final ReactiveMutationExecutorStandard INSTANCE = new ReactiveMutationExecutorStandard();

	private ReactiveMutationExecutorStandard() {
	}

	@Override
	public MutationExecutor createExecutor(
			Supplier<BatchKey> batchKeySupplier,
			MutationOperationGroup operationGroup,
			SharedSessionContractImplementor session) {
		return INSTANCE;
	}

	@Override
	public CompletionStage<Integer> executeReactive(
			JdbcOperationQueryMutation jdbcMutation,
			JdbcParameterBindings jdbcParameterBindings,
			Function<String, PreparedStatement> statementCreator,
			BiConsumer<Integer, PreparedStatement> expectationCheck,
			ExecutionContext executionContext) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		session.autoFlushIfRequired( jdbcMutation.getAffectedTableNames() );

		final LogicalConnectionImplementor logicalConnection = session
				.getJdbcCoordinator()
				.getLogicalConnection();

		final JdbcServices jdbcServices = session.getJdbcServices();
		final QueryOptions queryOptions = executionContext.getQueryOptions();
		final String finalSql = finalSql( jdbcMutation, executionContext, jdbcServices, queryOptions );


		Object[] parameters = PreparedStatementAdaptor.bind( statement -> prepareStatement(
				jdbcMutation,
				statement,
				jdbcParameterBindings,
				executionContext
		) );

		session.getEventListenerManager().jdbcExecuteStatementStart();

		return connection( executionContext )
				.update( finalSql, parameters )
				.thenApply( result -> {
					// FIXME: Should I have this check?
					// expectationCheck.accept( result, preparedStatement );
					return result;
				} )
				.whenComplete( (result, t) -> session.getEventListenerManager().jdbcExecuteStatementEnd() )
				.whenComplete( (result, t) -> executionContext.afterStatement( logicalConnection ) );
	}

	private void prepareStatement(
			JdbcOperationQueryMutation jdbcMutation,
			PreparedStatement preparedStatement,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		try {
			if ( executionContext.getQueryOptions().getTimeout() != null ) {
				preparedStatement.setQueryTimeout( executionContext.getQueryOptions().getTimeout() );
			}

			// bind parameters
			// 		todo : validate that all query parameters were bound?
			int paramBindingPosition = 1;
			for ( JdbcParameterBinder parameterBinder : jdbcMutation.getParameterBinders() ) {
				parameterBinder.bindParameterValue(
						preparedStatement,
						paramBindingPosition++,
						jdbcParameterBindings,
						executionContext
				);
			}
		}
		catch (SQLException sqle) {
			// I don't think this can happen
		}
	}

	private ReactiveConnection connection(ExecutionContext executionContext) {
		return ( (ReactiveConnectionSupplier) executionContext.getSession() ).getReactiveConnection();
	}

	private static String finalSql(
			JdbcOperationQueryMutation jdbcMutation,
			ExecutionContext executionContext,
			JdbcServices jdbcServices,
			QueryOptions queryOptions) {
		String sql = queryOptions == null
				? jdbcMutation.getSql()
				: jdbcServices.getDialect()
				.addSqlHintOrComment(
						jdbcMutation.getSql(),
						queryOptions,
						executionContext.getSession()
								.getFactory()
								.getSessionFactoryOptions()
								.isCommentsEnabled()
				);
		final Dialect dialect = executionContext.getSession().getJdbcServices().getDialect();
		return Parameters.instance( dialect ).process( sql );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.exec.spi.ReactiveMutationExecutor;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class StandardReactiveMutationExecutor implements ReactiveMutationExecutor {

	public static final StandardReactiveMutationExecutor INSTANCE = new StandardReactiveMutationExecutor();

	private StandardReactiveMutationExecutor() {
	}

	@Override
	public CompletionStage<Integer> execute(
			JdbcMutation jdbcMutation,
			JdbcParameterBindings jdbcParameterBindings,
			Function<String, PreparedStatement> statementCreator,
			BiConsumer<Integer, PreparedStatement> expectationCheck,
			ExecutionContext executionContext) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		session.autoFlushIfRequired( jdbcMutation.getAffectedTableNames() );

		final LogicalConnectionImplementor logicalConnection = session.getJdbcCoordinator().getLogicalConnection();
		final JdbcServices jdbcServices = session.getJdbcServices();
		final QueryOptions queryOptions = executionContext.getQueryOptions();
		final String finalSql = finalSql( jdbcMutation, executionContext, jdbcServices, queryOptions );

		try {
			// prepare the query
			final PreparedStatement preparedStatement = statementCreator.apply( finalSql );

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
		catch (Exception e) {
			// Temp workaround
		}

		throw new RuntimeException("Trust me, I know what I'm doing");
	}

	private static String finalSql(
			JdbcMutation jdbcMutation,
			ExecutionContext executionContext,
			JdbcServices jdbcServices,
			QueryOptions queryOptions) {
		return queryOptions == null
			? jdbcMutation.getSql()
			: jdbcServices.getDialect()
					.addSqlHintOrComment( jdbcMutation.getSql(), queryOptions, executionContext.getSession().getFactory().getSessionFactoryOptions().isCommentsEnabled() );
	}
}

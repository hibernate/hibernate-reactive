/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.internal;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.query.sql.internal.SQLQueryParser;
import org.hibernate.query.sql.spi.ParameterOccurrence;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutationNative;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

/**
 * @see org.hibernate.query.sql.internal.NativeNonSelectQueryPlanImpl
 */
public class ReactiveNativeNonSelectQueryPlan implements ReactiveNonSelectQueryPlan {

	private final String sql;
	private final Set<String> affectedTableNames;

	private final List<ParameterOccurrence> parameterList;

	public ReactiveNativeNonSelectQueryPlan(String sql, Set<String> affectedTableNames, List<ParameterOccurrence> parameterList) {
		this.sql = sql;
		this.affectedTableNames = affectedTableNames;
		this.parameterList = parameterList;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		ReactiveSharedSessionContractImplementor reactiveSession = (ReactiveSharedSessionContractImplementor) executionContext.getSession();
		return reactiveSession.reactiveAutoFlushIfRequired( affectedTableNames )
						.thenCompose( aBoolean -> {
							SharedSessionContractImplementor session = executionContext.getSession();
							BulkOperationCleanupAction.schedule( session, affectedTableNames );
							final List<JdbcParameterBinder> jdbcParameterBinders;
							final JdbcParameterBindings jdbcParameterBindings;

							final QueryParameterBindings queryParameterBindings = executionContext.getQueryParameterBindings();
							if ( parameterList == null || parameterList.isEmpty() ) {
								jdbcParameterBinders = Collections.emptyList();
								jdbcParameterBindings = JdbcParameterBindings.NO_BINDINGS;
							}
							else {
								jdbcParameterBinders = new ArrayList<>( parameterList.size() );
								jdbcParameterBindings = new JdbcParameterBindingsImpl(
										queryParameterBindings,
										parameterList,
										jdbcParameterBinders,
										session.getFactory()
								);
							}

							final SQLQueryParser parser = new SQLQueryParser( sql, null, session.getFactory() );

							final JdbcOperationQueryMutation jdbcMutation = new JdbcOperationQueryMutationNative(
									parser.process(),
									jdbcParameterBinders,
									affectedTableNames
							);

							return StandardReactiveJdbcMutationExecutor.INSTANCE
									.executeReactive(
											jdbcMutation,
											jdbcParameterBindings,
											sql -> session
													.getJdbcCoordinator()
													.getStatementPreparer()
													.prepareStatement( sql ),
											(integer, preparedStatement) -> {
											},
											SqmJdbcExecutionContextAdapter.usingLockingAndPaging( executionContext )
									);
						} );
	}
}

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
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveMutationExecutor;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.NativeJdbcMutation;

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
		final SharedSessionContractImplementor session = executionContext.getSession();
		session.autoFlushIfRequired( affectedTableNames );
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
			jdbcParameterBindings = new JdbcParameterBindingsImpl( parameterList.size() );

			jdbcParameterBindings.registerNativeQueryParameters(
					queryParameterBindings,
					parameterList,
					jdbcParameterBinders,
					session.getFactory()
			);
		}

		final SQLQueryParser parser = new SQLQueryParser( sql, null, session.getSessionFactory() );

		final JdbcMutation jdbcMutation = new NativeJdbcMutation(
				parser.process(),
				jdbcParameterBinders,
				affectedTableNames
		);

		return StandardReactiveMutationExecutor.INSTANCE
				.executeReactiveUpdate(
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
	}
}

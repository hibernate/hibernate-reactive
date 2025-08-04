/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.CacheableSqmInterpretation;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SimpleNonSelectQueryPlan;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.tree.SqmDmlStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.sql.ast.tree.MutationStatement;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import java.util.concurrent.CompletionStage;

public class ReactiveSimpleNonSelectQueryPlan extends
		SimpleNonSelectQueryPlan implements ReactiveNonSelectQueryPlan {

	public ReactiveSimpleNonSelectQueryPlan(
			SqmDmlStatement<?> statement,
			DomainParameterXref domainParameterXref) {
		super( statement, domainParameterXref );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext context) {
		BulkOperationCleanupAction.schedule( context.getSession(), getStatement() );
		final Interpretation interpretation = getInterpretation( context );
		final ExecutionContext executionContext = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context );
		return executeReactive( interpretation.interpretation(), interpretation.jdbcParameterBindings(), executionContext );
	}

	protected CompletionStage<Integer> executeReactive(CacheableSqmInterpretation<MutationStatement, JdbcOperationQueryMutation> sqmInterpretation, JdbcParameterBindings jdbcParameterBindings, ExecutionContext executionContext) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		return StandardReactiveJdbcMutationExecutor.INSTANCE.executeReactive(
				sqmInterpretation.jdbcOperation(),
				jdbcParameterBindings,
				sql -> session
						.getJdbcCoordinator()
						.getStatementPreparer()
						.prepareStatement( sql ),
				(integer, preparedStatement) -> {
				},
				executionContext
		);
	}
}

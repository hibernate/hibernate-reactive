/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.CacheableSqmInterpretation;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SimpleDeleteQueryPlan;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.reactive.action.internal.ReactiveBulkOperationCleanupAction;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.MutationStatement;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

public class ReactiveSimpleDeleteQueryPlan
		extends SimpleDeleteQueryPlan implements ReactiveNonSelectQueryPlan {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveSimpleDeleteQueryPlan(
			EntityPersister entityDescriptor,
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref) {
		super( entityDescriptor, sqmDelete, domainParameterXref );
	}

	@Override
	protected int execute(CacheableSqmInterpretation<MutationStatement, JdbcOperationQueryMutation> sqmInterpretation, JdbcParameterBindings jdbcParameterBindings, ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext context) {
		ReactiveBulkOperationCleanupAction.schedule( context.getSession(), getStatement() );
		final Interpretation interpretation = getInterpretation( context );
		final ExecutionContext executionContext = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context );
		return executeReactive( interpretation.interpretation(), interpretation.jdbcParameterBindings(), executionContext );
	}

	protected CompletionStage<Integer> executeReactive(CacheableSqmInterpretation<MutationStatement, JdbcOperationQueryMutation> sqmInterpretation, JdbcParameterBindings jdbcParameterBindings, ExecutionContext executionContext) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		return CompletionStages.loop(getCollectionTableDeletes(), delete ->
				executeReactive( delete, jdbcParameterBindings, executionContext.getSession(), executionContext )
		).thenCompose( v -> CompletionStages.loop(
							   getCollectionTableDeletes(), delete ->
								executeReactive(
										delete,
										jdbcParameterBindings,
										executionContext.getSession(),
										executionContext
								)
					   )
				.thenCompose( unused -> executeReactive(
						sqmInterpretation.jdbcOperation(),
						jdbcParameterBindings,
						session,
						executionContext
				) )
		);
	}

	private static CompletionStage<Integer> executeReactive(
			JdbcOperationQueryMutation delete,
			JdbcParameterBindings jdbcParameterBindings,
			SharedSessionContractImplementor executionContext,
			ExecutionContext executionContext1) {
		return StandardReactiveJdbcMutationExecutor.INSTANCE.executeReactive(
				delete,
				jdbcParameterBindings,
				sql -> executionContext
						.getJdbcCoordinator()
						.getStatementPreparer()
						.prepareStatement( sql ),
				(integer, preparedStatement) -> {},
				executionContext1
		);
	}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveAbstractMutationHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelect;

import java.util.concurrent.CompletionStage;

/**
 * @see org.hibernate.query.sqm.mutation.internal.cte.AbstractCteMutationHandler
 */
public interface ReactiveAbstractCteMutationHandler extends ReactiveAbstractMutationHandler {

	/**
	 * @see org.hibernate.query.sqm.mutation.internal.cte.AbstractCteMutationHandler#execute(JdbcParameterBindings, DomainQueryExecutionContext)
	 */
	@Override
	default CompletionStage<Integer> reactiveExecute(
			JdbcParameterBindings jdbcParameterBindings,
			DomainQueryExecutionContext executionContext){
		final LockOptions lockOptions = executionContext.getQueryOptions().getLockOptions();
		// Acquire a WRITE lock for the rows that are about to be modified
		lockOptions.setLockMode( LockMode.WRITE );
		return ( (ReactiveSharedSessionContractImplementor) executionContext.getSession() )
				.reactiveAutoFlushIfRequired( getSelect().getAffectedTableNames() )
				.thenCompose( v -> StandardReactiveSelectExecutor.INSTANCE
						.list(
								getSelect(),
								jdbcParameterBindings,
								SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( executionContext ),
								row -> row[0],
								null,
								ReactiveListResultsConsumer.UniqueSemantic.NONE,
								1
						)
						.thenApply( list -> ( (Number) list.get( 0 ) ).intValue() )
				);
	}

	JdbcSelect getSelect();
}

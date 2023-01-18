/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.util.concurrent.CompletionStage;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;

/**
 * @see org.hibernate.query.sqm.internal.MultiTableDeleteQueryPlan
 */
public class ReactiveMultiTableDeleteQueryPlan implements ReactiveNonSelectQueryPlan {
	private final SqmDeleteStatement sqmDelete;
	private final DomainParameterXref domainParameterXref;
	private final ReactiveSqmMultiTableMutationStrategy deleteStrategy;

	public ReactiveMultiTableDeleteQueryPlan(
			SqmDeleteStatement sqmDelete,
			DomainParameterXref domainParameterXref,
			ReactiveSqmMultiTableMutationStrategy deleteStrategy) {
		this.sqmDelete = sqmDelete;
		this.domainParameterXref = domainParameterXref;
		this.deleteStrategy = deleteStrategy;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmDelete );
		return deleteStrategy.reactiveExecuteDelete( sqmDelete, domainParameterXref, executionContext );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.util.concurrent.CompletionStage;

import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;

/**
 * @see org.hibernate.query.sqm.internal.MultiTableUpdateQueryPlan
 */
public class ReactiveMultiTableUpdateQueryPlan implements ReactiveNonSelectQueryPlan {
	private final SqmUpdateStatement sqmUpdate;
	private final DomainParameterXref domainParameterXref;
	private final SqmMultiTableMutationStrategy mutationStrategy;

	public ReactiveMultiTableUpdateQueryPlan(
			SqmUpdateStatement sqmUpdate,
			DomainParameterXref domainParameterXref,
			SqmMultiTableMutationStrategy mutationStrategy) {
		this.sqmUpdate = sqmUpdate;
		this.domainParameterXref = domainParameterXref;
		this.mutationStrategy = mutationStrategy;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
//		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmUpdate );
//		return mutationStrategy.executeUpdate( sqmUpdate, domainParameterXref, executionContext );
		throw new NotYetImplementedFor6Exception();
	}
}

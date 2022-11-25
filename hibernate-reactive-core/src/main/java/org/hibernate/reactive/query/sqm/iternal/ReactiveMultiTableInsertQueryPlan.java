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
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;

/**
 * @see org.hibernate.query.sqm.internal.MultiTableInsertQueryPlan
 */
public class ReactiveMultiTableInsertQueryPlan implements ReactiveNonSelectQueryPlan {
	private final SqmInsertStatement<?> sqmInsert;
	private final DomainParameterXref domainParameterXref;
	private final SqmMultiTableInsertStrategy mutationStrategy;

	public ReactiveMultiTableInsertQueryPlan(
			SqmInsertStatement<?> sqmInsert,
			DomainParameterXref domainParameterXref,
			SqmMultiTableInsertStrategy mutationStrategy) {
		this.sqmInsert = sqmInsert;
		this.domainParameterXref = domainParameterXref;
		this.mutationStrategy = mutationStrategy;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
//		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmInsert );
//		return mutationStrategy.executeInsert( sqmInsert, domainParameterXref, executionContext );
		throw new NotYetImplementedFor6Exception();
	}
}

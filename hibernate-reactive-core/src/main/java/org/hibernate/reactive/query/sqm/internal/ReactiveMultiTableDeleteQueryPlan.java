/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;

/**
 * @see org.hibernate.query.sqm.internal.MultiTableDeleteQueryPlan
 */
public class ReactiveMultiTableDeleteQueryPlan
		extends ReactiveAbstractMultiTableMutationQueryPlan<SqmDeleteStatement<?>, SqmMultiTableMutationStrategy> {

	public ReactiveMultiTableDeleteQueryPlan(
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref,
			ReactiveSqmMultiTableMutationStrategy deleteStrategy) {
		super( sqmDelete, domainParameterXref, deleteStrategy );
	}

	@Override
	protected MultiTableHandlerBuildResult buildHandler(
			SqmDeleteStatement<?> statement,
			DomainParameterXref domainParameterXref,
			SqmMultiTableMutationStrategy strategy,
			DomainQueryExecutionContext context) {
		return strategy.buildHandler( statement, domainParameterXref, context );
	}
}

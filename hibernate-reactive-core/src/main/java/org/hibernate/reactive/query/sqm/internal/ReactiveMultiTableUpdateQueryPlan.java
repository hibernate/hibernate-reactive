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
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;

/**
 * @see org.hibernate.query.sqm.internal.MultiTableUpdateQueryPlan
 */
public class ReactiveMultiTableUpdateQueryPlan
		extends ReactiveAbstractMultiTableMutationQueryPlan<SqmUpdateStatement<?>, SqmMultiTableMutationStrategy> {

	public ReactiveMultiTableUpdateQueryPlan(
			SqmUpdateStatement<?> sqmUpdate,
			DomainParameterXref domainParameterXref,
			SqmMultiTableMutationStrategy mutationStrategy) {
		super( sqmUpdate, domainParameterXref, mutationStrategy );
	}

	@Override
	protected MultiTableHandlerBuildResult buildHandler(
			SqmUpdateStatement<?> statement,
			DomainParameterXref domainParameterXref,
			SqmMultiTableMutationStrategy strategy,
			DomainQueryExecutionContext context) {
		return strategy.buildHandler( statement, domainParameterXref, context );
	}
}

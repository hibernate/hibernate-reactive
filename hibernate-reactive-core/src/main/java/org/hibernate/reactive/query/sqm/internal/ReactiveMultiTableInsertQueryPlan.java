/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;

/**
 * @see org.hibernate.query.sqm.internal.MultiTableInsertQueryPlan
 */
public class ReactiveMultiTableInsertQueryPlan
		extends ReactiveAbstractMultiTableMutationQueryPlan<SqmInsertStatement<?>, SqmMultiTableInsertStrategy> {

	public ReactiveMultiTableInsertQueryPlan(
			SqmInsertStatement<?> sqmInsert,
			DomainParameterXref domainParameterXref,
			ReactiveSqmMultiTableInsertStrategy mutationStrategy) {
		super( sqmInsert, domainParameterXref, mutationStrategy );
	}

	@Override
	protected MultiTableHandlerBuildResult buildHandler(
			SqmInsertStatement<?> statement,
			DomainParameterXref domainParameterXref,
			SqmMultiTableInsertStrategy strategy,
			DomainQueryExecutionContext context) {
		return strategy.buildHandler( statement, domainParameterXref, context );
	}
}

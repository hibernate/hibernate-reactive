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
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;

/**
 * @see org.hibernate.query.sqm.internal.SimpleInsertQueryPlan
 */
public class ReactiveSimpleInsertQueryPlan implements ReactiveNonSelectQueryPlan {

	private final SqmInsertStatement<?> sqmInsert;

	private final DomainParameterXref domainParameterXref;

	public ReactiveSimpleInsertQueryPlan(
			SqmInsertStatement<?> sqmInsert,
			DomainParameterXref domainParameterXref) {
		this.sqmInsert = sqmInsert;
		this.domainParameterXref = domainParameterXref;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		throw new NotYetImplementedFor6Exception();
	}
}

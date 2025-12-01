/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.AbstractMultiTableMutationQueryPlan;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.tree.SqmDmlStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;

import java.util.concurrent.CompletionStage;

public abstract class ReactiveAbstractMultiTableMutationQueryPlan<S extends SqmDmlStatement<?>, F>
		extends AbstractMultiTableMutationQueryPlan<S, F> implements ReactiveNonSelectQueryPlan {

	public ReactiveAbstractMultiTableMutationQueryPlan(
			S statement,
			DomainParameterXref domainParameterXref,
			F strategy) {
		super( statement, domainParameterXref, strategy );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext context) {
		BulkOperationCleanupAction.schedule( context.getSession(), getStatement() );
		final Interpretation interpretation = getInterpretation( context );
		return ((ReactiveHandler)interpretation.handler()).reactiveExecute( interpretation.jdbcParameterBindings(), context );
	}

}

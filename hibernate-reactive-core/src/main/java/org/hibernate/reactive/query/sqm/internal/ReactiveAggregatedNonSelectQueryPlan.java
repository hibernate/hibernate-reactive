/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;

import static org.hibernate.reactive.util.impl.CompletionStages.total;

/**
 * @see org.hibernate.query.sqm.internal.AggregatedNonSelectQueryPlanImpl
 */
public class ReactiveAggregatedNonSelectQueryPlan implements ReactiveNonSelectQueryPlan {

	private final ReactiveNonSelectQueryPlan[] aggregatedQueryPlans;

	public ReactiveAggregatedNonSelectQueryPlan(ReactiveNonSelectQueryPlan[] aggregatedQueryPlans) {
		this.aggregatedQueryPlans = aggregatedQueryPlans;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		return total( aggregatedQueryPlans, nonSelectQueryPlan -> nonSelectQueryPlan
				.executeReactiveUpdate( executionContext ) );
	}
}

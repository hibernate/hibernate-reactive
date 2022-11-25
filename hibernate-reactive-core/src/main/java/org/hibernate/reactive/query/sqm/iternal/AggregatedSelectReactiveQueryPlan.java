/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.Limit;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;

import static java.util.Collections.emptyList;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;

/**
 * @see org.hibernate.query.sqm.internal.AggregatedSelectQueryPlanImpl
 */
public class AggregatedSelectReactiveQueryPlan<R> implements ReactiveSelectQueryPlan<R> {

	private final ReactiveSelectQueryPlan<R>[] aggregatedQueryPlans;

	public AggregatedSelectReactiveQueryPlan(ReactiveSelectQueryPlan<R>[] aggregatedQueryPlans) {
		this.aggregatedQueryPlans = aggregatedQueryPlans;
	}

	@Override
	public CompletionStage<List<R>> performReactiveList(DomainQueryExecutionContext executionContext) {
		final Limit effectiveLimit = executionContext.getQueryOptions().getEffectiveLimit();
		final int maxRowsJpa = effectiveLimit.getMaxRowsJpa();
		if ( maxRowsJpa == 0 ) {
			return completedFuture( emptyList() );
		}
		final AtomicInteger elementsToSkip = new AtomicInteger( effectiveLimit.getFirstRowJpa() );
		final List<R> overallResults = new ArrayList<>();

		return whileLoop( aggregatedQueryPlans, reactivePlan -> reactivePlan
				.performReactiveList( executionContext )
				.thenApply( list -> updateResults( elementsToSkip, maxRowsJpa, overallResults, list ) )
		).thenApply( v -> overallResults );
	}

	private static <R> boolean updateResults(AtomicInteger elementsToSkipAtomic, int maxRowsJpa, List<R> overallResults, List<R> list) {
		final int size = list.size();
		if ( size <= elementsToSkipAtomic.get() ) {
			// More elements to skip than the collection size
			elementsToSkipAtomic.addAndGet( -size );
			return true;
		}
		final int elementsToSkip = elementsToSkipAtomic.get();
		final int availableElements = size - elementsToSkip;
		if ( overallResults.size() + availableElements >= maxRowsJpa ) {
			// This result list is the last one i.e. fulfills the limit
			final int end = elementsToSkip + ( maxRowsJpa - overallResults.size() );
			for ( int i = elementsToSkip; i < end; i++ ) {
				overallResults.add( list.get( i ) );
			}
			return false;
		}
		else if ( elementsToSkip > 0 ) {
			// We can skip a part of this result list
			for ( int i = availableElements; i < size; i++ ) {
				overallResults.add( list.get( i ) );
			}
			elementsToSkipAtomic.set( 0 );
		}
		else {
			overallResults.addAll( list );
		}
		return true;
	}
}

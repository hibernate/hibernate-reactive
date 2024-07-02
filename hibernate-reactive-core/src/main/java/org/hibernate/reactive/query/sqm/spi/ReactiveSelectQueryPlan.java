/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.spi;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.ScrollMode;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.ScrollableResultsImplementor;
import org.hibernate.query.spi.SelectQueryPlan;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.sql.results.spi.ReactiveResultsConsumer;
import org.hibernate.sql.results.spi.ResultsConsumer;

import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

/**
 * @see org.hibernate.query.spi.SelectQueryPlan
 */
public interface ReactiveSelectQueryPlan<R> extends SelectQueryPlan<R> {

	@Override
	default List<R> performList(DomainQueryExecutionContext executionContext) {
		throw make( Log.class, MethodHandles.lookup() )
				.nonReactiveMethodCall( "performReactiveList" );
	}

	@Override
	default	ScrollableResultsImplementor<R> performScroll(ScrollMode scrollMode, DomainQueryExecutionContext executionContext) {
		throw make( Log.class, MethodHandles.lookup() )
				.nonReactiveMethodCall( "<no alternative>" );
	}

	default <T> T executeQuery(DomainQueryExecutionContext executionContext, ResultsConsumer<T, R> resultsConsumer) {
		throw make( Log.class, MethodHandles.lookup() )
				.nonReactiveMethodCall( "reactiveExecuteQuery" );
	}

	/**
	 * Execute the query
	 */
	default <T> CompletionStage<T> reactiveExecuteQuery(DomainQueryExecutionContext executionContext, ReactiveResultsConsumer<T, R> resultsConsumer) {
		return failedFuture( new UnsupportedOperationException() );
	}

	/**
	 * Perform (execute) the query returning a List
	 */
	CompletionStage<List<R>> reactivePerformList(DomainQueryExecutionContext executionContext);
}

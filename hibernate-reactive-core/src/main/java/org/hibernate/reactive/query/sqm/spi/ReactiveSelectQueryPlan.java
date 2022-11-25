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
import org.hibernate.reactive.logging.impl.LoggerFactory;

/**
 * @see org.hibernate.query.spi.SelectQueryPlan
 */
public interface ReactiveSelectQueryPlan<R> extends SelectQueryPlan<R> {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default List<R> performList(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "performReactiveList" );
	}

	@Override
	default	ScrollableResultsImplementor<R> performScroll(ScrollMode scrollMode, DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "<no alternative>" );
	}

	/**
	 * Perform (execute) the query returning a List
	 */
	CompletionStage<List<R>> performReactiveList(DomainQueryExecutionContext executionContext);
}

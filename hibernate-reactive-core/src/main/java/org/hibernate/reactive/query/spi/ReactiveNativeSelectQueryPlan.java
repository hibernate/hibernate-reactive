/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.spi;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.hibernate.ScrollMode;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.ScrollableResultsImplementor;
import org.hibernate.query.sql.spi.NativeSelectQueryPlan;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;

public interface ReactiveNativeSelectQueryPlan<T> extends NativeSelectQueryPlan, ReactiveSelectQueryPlan {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * @deprecated  not a reactive method
	 */
	@Override
	@Deprecated
	default List<T> performList(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "performReactiveList" );
	}

	/**
	 * @deprecated  not a reactive method
	 */
	@Override
	@Deprecated
	default ScrollableResultsImplementor<T> performScroll(ScrollMode scrollMode, DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "<no alternative>" );
	}
}

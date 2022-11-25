package org.hibernate.reactive.query.spi;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;

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
	default List<R> performList(DomainQueryExecutionContext executionContext) {
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

	CompletionStage<List<T>> performReactiveList();
}

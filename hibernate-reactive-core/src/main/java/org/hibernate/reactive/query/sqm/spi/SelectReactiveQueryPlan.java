package org.hibernate.reactive.query.sqm.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryPlan;

/**
 * @see org.hibernate.query.spi.SelectQueryPlan
 */
public interface SelectReactiveQueryPlan<R> extends QueryPlan {
	/**
	 * Perform (execute) the query returning a List
	 */
	CompletionStage<List<R>> performList(DomainQueryExecutionContext executionContext);
}

package org.hibernate.reactive.query.sqm.iternal;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.SelectQueryPlan;
import org.hibernate.reactive.query.sqm.spi.SelectReactiveQueryPlan;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class ConcreteSqmSelectReactiveQueryPlan<R> implements SelectReactiveQueryPlan<R> {
	private final SelectQueryPlan<R> delegate;

	public ConcreteSqmSelectReactiveQueryPlan(SelectQueryPlan<R> delegate) {
		this.delegate = delegate;
	}

	@Override
	public CompletionStage<List<R>> performList(DomainQueryExecutionContext executionContext) {
		if ( executionContext.getQueryOptions().getEffectiveLimit().getMaxRowsJpa() == 0 ) {
			return completedFuture( Collections.emptyList() );
		}
		return null;
	}
}

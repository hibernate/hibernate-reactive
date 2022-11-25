package org.hibernate.reactive.query.sql.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.NonSelectQueryPlan;

/**
 * A reactive {@link org.hibernate.query.spi.NonSelectQueryPlan}
 */
public interface ReactiveNonSelectQueryPlan extends NonSelectQueryPlan {

	CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext);

}

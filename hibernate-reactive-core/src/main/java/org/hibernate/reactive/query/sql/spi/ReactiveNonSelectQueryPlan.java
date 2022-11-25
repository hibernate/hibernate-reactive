/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.NonSelectQueryPlan;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

/**
 * A reactive {@link org.hibernate.query.spi.NonSelectQueryPlan}
 */
//FIXME: Maybe it doesn't need to extend NonSelectQueryPlan (just QueryPlan)
public interface ReactiveNonSelectQueryPlan extends NonSelectQueryPlan {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default int executeUpdate(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext);

}

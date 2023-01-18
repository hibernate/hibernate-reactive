/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;

/**
 * @see org.hibernate.query.sqm.mutation.internal.Handler
 */
public interface ReactiveHandler {

	/**
	 * Execute the multi-table update or delete indicated by the SQM AST
	 * passed in when this Handler was created.
	 *
	 * @param executionContext Contextual information needed for execution
	 *
	 * @return The "number of rows affected" count
	 */
	CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext);
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleSelfExecuting;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.SelfExecutingUpdateOperation;

public class ReactiveMutationExecutorSingleSelfExecuting extends MutationExecutorSingleSelfExecuting
		implements ReactiveMutationExecutor {

	public ReactiveMutationExecutorSingleSelfExecuting(SelfExecutingUpdateOperation operation) {
		super( operation );
	}
}

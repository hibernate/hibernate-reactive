/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsert;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.model.MutationOperationGroup;

public class ReactiveMutationExecutorPostInsert extends MutationExecutorPostInsert implements MutationExecutor {

	public ReactiveMutationExecutorPostInsert(
			MutationOperationGroup mutationOperationGroup,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, session );
	}
}

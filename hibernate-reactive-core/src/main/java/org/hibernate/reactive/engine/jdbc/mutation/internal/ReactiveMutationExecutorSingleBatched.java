/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleBatched;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.model.PreparableMutationOperation;

public class ReactiveMutationExecutorSingleBatched extends MutationExecutorSingleBatched implements MutationExecutor {

	public ReactiveMutationExecutorSingleBatched(
			PreparableMutationOperation mutationOperation,
			BatchKey batchKey,
			int batchSize,
			SharedSessionContractImplementor session) {
		super( mutationOperation, batchKey, batchSize, session );
	}
}

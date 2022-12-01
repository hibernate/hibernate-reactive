/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation.internal;

import java.util.function.Supplier;

import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.model.MutationOperationGroup;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard
 */
public class ReactiveMutationExecutorStandard extends MutationExecutorStandard {
	public ReactiveMutationExecutorStandard(
			MutationOperationGroup mutationOperationGroup,
			Supplier<BatchKey> batchKeySupplier,
			int batchSize,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, batchKeySupplier, batchSize, session );
	}
}

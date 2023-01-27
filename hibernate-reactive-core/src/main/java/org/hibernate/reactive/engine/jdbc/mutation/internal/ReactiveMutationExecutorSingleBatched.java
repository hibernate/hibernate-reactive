/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleBatched;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveMutationExecutorSingleBatched extends MutationExecutorSingleBatched implements
		ReactiveMutationExecutor {

	public ReactiveMutationExecutorSingleBatched(
			PreparableMutationOperation mutationOperation,
			BatchKey batchKey,
			int batchSize,
			SharedSessionContractImplementor session) {
		super( mutationOperation, batchKey, batchSize, session );
	}

	@Override
	public CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker) {
		super.performBatchedOperations( valuesAnalysis, inclusionChecker );
		return voidFuture();
	}
}

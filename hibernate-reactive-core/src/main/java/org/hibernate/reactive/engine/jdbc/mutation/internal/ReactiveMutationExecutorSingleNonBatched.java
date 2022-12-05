/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleNonBatched;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.ValuesAnalysis;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleNonBatched
 */
public class ReactiveMutationExecutorSingleNonBatched extends MutationExecutorSingleNonBatched
		implements ReactiveMutationExecutor {

	public ReactiveMutationExecutorSingleNonBatched(
			PreparableMutationOperation mutationOperation,
			SharedSessionContractImplementor session) {
		super( mutationOperation, session );
	}

	@Override
	public CompletionStage<Void> performReactiveNonBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return performReactiveNonBatchedMutation(
				getStatementGroup().getSingleStatementDetails(),
				getJdbcValueBindings(),
				inclusionChecker,
				resultChecker,
				session );
	}

	@Override
	public void release() {
	}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleNonBatched;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.generator.values.ReactiveGeneratedValuesMutationDelegate;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.ValuesAnalysis;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleNonBatched
 */
public class ReactiveMutationExecutorSingleNonBatched extends MutationExecutorSingleNonBatched
		implements ReactiveMutationExecutor {

	private final ReactiveGeneratedValuesMutationDelegate generatedValuesDelegate;

	public ReactiveMutationExecutorSingleNonBatched(
			PreparableMutationOperation mutationOperation,
			GeneratedValuesMutationDelegate generatedValuesDelegate,
			SharedSessionContractImplementor session) {
		super( mutationOperation, generatedValuesDelegate, session );
		this.generatedValuesDelegate = (ReactiveGeneratedValuesMutationDelegate) generatedValuesDelegate;
	}

	@Override
	public CompletionStage<GeneratedValues> performReactiveNonBatchedOperations(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session,
			boolean isIdentityInsert,
			String[] identifierColumnsNames) {
		PreparedStatementDetails singleStatementDetails = getStatementGroup().getSingleStatementDetails();
		if ( generatedValuesDelegate != null ) {
			return generatedValuesDelegate.reactivePerformMutation(
					singleStatementDetails,
					getJdbcValueBindings(),
					modelReference,
					session
			);
		}
		return performReactiveNonBatchedMutation(
				singleStatementDetails,
				null,
				getJdbcValueBindings(),
				inclusionChecker,
				resultChecker,
				session,
				identifierColumnsNames
		).thenCompose( CompletionStages::nullFuture );
	}

	@Override
	public void release() {
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsertSingleTable;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.jdbc.Expectation;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;
import org.hibernate.sql.model.jdbc.JdbcInsertMutation;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.identityPreparation;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_TRACE_ENABLED;


public class ReactiveMutationExecutorPostInsertSingleTable extends MutationExecutorPostInsertSingleTable
		implements ReactiveMutationExecutor {

	private final EntityMutationTarget mutationTarget;
	private final PreparedStatementDetails identityInsertStatementDetails;

	public ReactiveMutationExecutorPostInsertSingleTable(MutationOperationGroup mutationOperationGroup, SharedSessionContractImplementor session) {
		super( mutationOperationGroup, session );
		this.mutationTarget = (EntityMutationTarget) mutationOperationGroup.getMutationTarget();
		final PreparableMutationOperation operation = mutationOperationGroup.getOperation( mutationTarget.getIdentifierTableName() );
		this.identityInsertStatementDetails = identityPreparation( operation, session );
	}

	private static class ReactiveIdentityInsertMutation extends JdbcInsertMutation {

		public ReactiveIdentityInsertMutation(
				TableMapping tableDetails,
				MutationTarget<?> mutationTarget,
				String sql,
				boolean callable,
				Expectation expectation,
				List<? extends JdbcParameterBinder> parameterBinders) {
			super( tableDetails, mutationTarget, sql, callable, expectation, parameterBinders );
		}
	}

	@Override
	public CompletionStage<Object> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		final Object id = mutationTarget.getIdentityInsertDelegate()
				.performInsert( identityInsertStatementDetails, getJdbcValueBindings(), modelReference, session );

		// We should change the signature of performInsert and always return a CompletionStage.
		CompletionStage<Object> idStage = id instanceof CompletionStage
				? (CompletionStage<Object>) id
				: completedFuture( id );

		return idStage.thenApply( this::logId );
	}

	private Object logId(Object identifier) {
		if ( MODEL_MUTATION_LOGGER_TRACE_ENABLED ) {
			MODEL_MUTATION_LOGGER
					.tracef( "Post-insert generated value : `%s` (%s)", identifier, mutationTarget.getNavigableRole().getFullPath() );
		}
		return identifier;
	}
}

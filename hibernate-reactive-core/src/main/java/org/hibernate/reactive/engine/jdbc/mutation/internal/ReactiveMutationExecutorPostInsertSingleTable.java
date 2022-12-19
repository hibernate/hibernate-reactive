/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsertSingleTable;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.ValuesAnalysis;

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

	@Override
	public CompletionStage<Object> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		final InsertGeneratedIdentifierDelegate identityHandler = mutationTarget.getIdentityInsertDelegate();
		final Object id = identityHandler
				.performInsert( identityInsertStatementDetails, getJdbcValueBindings(), modelReference, session );

		if ( MODEL_MUTATION_LOGGER_TRACE_ENABLED ) {
			MODEL_MUTATION_LOGGER.tracef( "Post-insert generated value : `%s` (%s)", id, mutationTarget.getNavigableRole().getFullPath() );
		}

		return completedFuture( id );
	}
}

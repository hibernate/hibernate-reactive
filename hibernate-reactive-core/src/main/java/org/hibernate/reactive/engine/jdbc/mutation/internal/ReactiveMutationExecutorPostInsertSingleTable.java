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
import org.hibernate.engine.jdbc.mutation.internal.EntityMutationOperationGroup;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsertSingleTable;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.id.insert.ReactiveInsertGeneratedIdentifierDelegate;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.identityPreparation;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

public class ReactiveMutationExecutorPostInsertSingleTable extends MutationExecutorPostInsertSingleTable
		implements ReactiveMutationExecutor {

	private final EntityMutationTarget mutationTarget;
	private final PreparedStatementDetails identityInsertStatementDetails;

	public ReactiveMutationExecutorPostInsertSingleTable(EntityMutationOperationGroup mutationOperationGroup, SharedSessionContractImplementor session) {
		super( mutationOperationGroup, session );
		this.mutationTarget = mutationOperationGroup.getMutationTarget();
		final PreparableMutationOperation operation = (PreparableMutationOperation) mutationOperationGroup
				.getOperation( mutationTarget.getIdentifierTableName() );
		this.identityInsertStatementDetails = identityPreparation( operation, session );
	}

	@Override
	public CompletionStage<Object> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return ( (ReactiveInsertGeneratedIdentifierDelegate) mutationTarget.getIdentityInsertDelegate() )
				.reactivePerformInsert(
						identityInsertStatementDetails,
						getJdbcValueBindings(),
						modelReference,
						session
				)
				.thenApply( this::logId );
	}

	private Object logId(Object identifier) {
		if ( MODEL_MUTATION_LOGGER.isTraceEnabled() ) {
			MODEL_MUTATION_LOGGER
					.tracef( "Post-insert generated value : `%s` (%s)", identifier, mutationTarget.getNavigableRole().getFullPath() );
		}
		return identifier;
	}
}

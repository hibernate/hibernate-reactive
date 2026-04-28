/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorStandard;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.persister.entity.mutation.TemporalMutationHelper;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.model.MutationOperationGroup;

import static org.hibernate.persister.collection.mutation.RowMutationOperations.DEFAULT_RESTRICTOR;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

public class ReactiveRemoveCoordinatorStandard extends RemoveCoordinatorStandard implements ReactiveRemoveCoordinator {

	private MutationOperationGroup operationGroup;

	public ReactiveRemoveCoordinatorStandard(
			CollectionMutationTarget mutationTarget,
			RowMutationOperations mutationOperations,
			ServiceRegistry serviceRegistry) {
		super( mutationTarget, mutationOperations, serviceRegistry );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAllRows(Object key, SharedSessionContractImplementor session) {
		if ( MODEL_MUTATION_LOGGER.isTraceEnabled() ) {
			MODEL_MUTATION_LOGGER.removingCollection( getMutationTarget().getRolePath(), key );
		}

		if ( operationGroup == null ) {
			// delayed creation of the operation-group
			operationGroup = buildOperationGroup();
		}

		final ReactiveMutationExecutor mutationExecutor = reactiveMutationExecutor( session, operationGroup );
		return supplyStage( () -> {
					final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
					final ForeignKeyDescriptor fkDescriptor = getMutationTarget().getTargetPart().getKeyDescriptor();
					fkDescriptor.getKeyPart()
							.decompose( key, 0, jdbcValueBindings, null, DEFAULT_RESTRICTOR, session );

					final var temporalMapping = getMutationTarget().getTargetPart().getTemporalMapping();
					if ( temporalMapping != null && TemporalMutationHelper.isUsingParameters( session ) ) {
						jdbcValueBindings.bindValue(
								session.getCurrentTransactionIdentifier(),
								temporalMapping.getEndingColumnMapping(),
								ParameterUsage.SET
						);
					}
					return mutationExecutor
							.executeReactive( key, null, null, null, session );
				} )
				.whenComplete( (o, throwable) -> mutationExecutor.release() )
				.thenCompose( CompletionStages::voidFuture );
	}


	private ReactiveMutationExecutor reactiveMutationExecutor(SharedSessionContractImplementor session, MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		return  (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getBatchKey, operationGroup, session );
	}
}

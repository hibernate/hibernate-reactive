/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.persister.collection.AbstractCollectionPersister;
import org.hibernate.persister.collection.mutation.OperationProducer;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorStandard;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.persister.entity.mutation.TemporalMutationHelper;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.ast.MutatingTableReference;

import static org.hibernate.persister.collection.mutation.RowMutationOperations.DEFAULT_RESTRICTOR;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.internal.MutationOperationGroupFactory.singleOperation;

public class ReactiveRemoveCoordinatorStandard extends RemoveCoordinatorStandard implements ReactiveRemoveCoordinator {

	private MutationOperationGroup operationGroup;
	private final OperationProducer operationProducer;
	private final BasicBatchKey batchKey;

	public ReactiveRemoveCoordinatorStandard(
			AbstractCollectionPersister mutationTarget,
			RowMutationOperations mutationOperations,
			ServiceRegistry serviceRegistry) {
		super( mutationTarget, mutationOperations, serviceRegistry );
		this.operationProducer = mutationOperations.getDeleteAllRowsOperationProducer();
		this.batchKey = new BasicBatchKey( getMutationTarget().getRolePath() + "#REMOVE" );
	}

	private MutationOperationGroup buildOperationGroup() {
		assert getMutationTarget().getTargetPart() != null
			&& getMutationTarget().getTargetPart().getKeyDescriptor() != null;

		final var tableMapping = getMutationTarget().getCollectionTableMapping();
		final var tableReference = new MutatingTableReference( tableMapping );
		return singleOperation( MutationType.DELETE, getMutationTarget(),
				operationProducer.createOperation( tableReference ) );
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
								session.getCurrentChangesetIdentifier(),
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
				.createExecutor( () -> batchKey, operationGroup, session );
	}
}

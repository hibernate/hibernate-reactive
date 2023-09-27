/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.CollectionTableMapping;
import org.hibernate.persister.collection.mutation.OperationProducer;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorStandard;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.ast.MutatingTableReference;

import static org.hibernate.persister.collection.mutation.RowMutationOperations.DEFAULT_RESTRICTOR;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.MutationType.DELETE;
import static org.hibernate.sql.model.internal.MutationOperationGroupFactory.singleOperation;

public class ReactiveRemoveCoordinatorStandard extends RemoveCoordinatorStandard implements ReactiveRemoveCoordinator {

	private final BasicBatchKey batchKey;
	private final OperationProducer operationProducer;
	private MutationOperationGroup operationGroup;

	public ReactiveRemoveCoordinatorStandard(
			CollectionMutationTarget mutationTarget,
			OperationProducer operationProducer,
			ServiceRegistry serviceRegistry) {
		super( mutationTarget, operationProducer, serviceRegistry );
		this.batchKey = new BasicBatchKey( mutationTarget.getRolePath() + "#REMOVE" );
		this.operationProducer = operationProducer;
	}

	private BasicBatchKey getBatchKey() {
		return batchKey;
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAllRows(Object key, SharedSessionContractImplementor session) {
		if ( MODEL_MUTATION_LOGGER.isDebugEnabled() ) {
			MODEL_MUTATION_LOGGER
					.debugf( "Deleting collection - %s : %s", getMutationTarget().getRolePath(), key );
		}

		if ( operationGroup == null ) {
			// delayed creation of the operation-group
			operationGroup = buildOperationGroup();
		}

		final ReactiveMutationExecutor mutationExecutor = reactiveMutationExecutor( session, operationGroup );

		return voidFuture()
				.thenCompose( unused -> {
					final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
					final ForeignKeyDescriptor fkDescriptor = getMutationTarget().getTargetPart().getKeyDescriptor();
					fkDescriptor.getKeyPart()
							.decompose( key, 0, jdbcValueBindings, null, DEFAULT_RESTRICTOR, session );

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

	// FIXME: Update ORM and inherit this
	private MutationOperationGroup buildOperationGroup() {
		assert getMutationTarget().getTargetPart() != null;
		assert getMutationTarget().getTargetPart().getKeyDescriptor() != null;

		if ( MODEL_MUTATION_LOGGER.isTraceEnabled() ) {
			MODEL_MUTATION_LOGGER.tracef( "Starting RemoveCoordinator#buildOperationGroup - %s", getMutationTarget().getRolePath() );
		}

		final CollectionTableMapping tableMapping = getMutationTarget().getCollectionTableMapping();
		final MutatingTableReference tableReference = new MutatingTableReference( tableMapping );

		return singleOperation( DELETE, getMutationTarget(), operationProducer.createOperation( tableReference ) );
	}
}

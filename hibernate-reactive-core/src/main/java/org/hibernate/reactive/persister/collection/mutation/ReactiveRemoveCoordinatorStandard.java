/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.CollectionTableMapping;
import org.hibernate.persister.collection.mutation.OperationProducer;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorStandard;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.ast.MutatingTableReference;
import org.hibernate.sql.model.internal.MutationOperationGroupSingle;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_DEBUG_ENABLED;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_TRACE_ENABLED;

public class ReactiveRemoveCoordinatorStandard extends RemoveCoordinatorStandard implements ReactiveRemoveCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final BasicBatchKey batchKey;
	private final OperationProducer operationProducer;
	private MutationOperationGroupSingle operationGroup;

	public ReactiveRemoveCoordinatorStandard(CollectionMutationTarget mutationTarget, OperationProducer operationProducer) {
		super( mutationTarget, operationProducer );
		this.batchKey = new BasicBatchKey( mutationTarget.getRolePath() + "#REMOVE" );
		this.operationProducer = operationProducer;
	}

	private BasicBatchKey getBatchKey() {
		return batchKey;
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAllRows(Object key, SharedSessionContractImplementor session) {
		if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
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
					fkDescriptor.getKeyPart().decompose(
							key,
							(jdbcValue, jdbcValueMapping) -> jdbcValueBindings
									.bindValue( jdbcValue, jdbcValueMapping, ParameterUsage.RESTRICT
							),
							session
					);

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
	protected MutationOperationGroupSingle buildOperationGroup() {
		assert getMutationTarget().getTargetPart() != null;
		assert getMutationTarget().getTargetPart().getKeyDescriptor() != null;

		if ( MODEL_MUTATION_LOGGER_TRACE_ENABLED ) {
			MODEL_MUTATION_LOGGER.tracef( "Starting RemoveCoordinator#buildOperationGroup - %s", getMutationTarget().getRolePath() );
		}

		final CollectionTableMapping tableMapping = getMutationTarget().getCollectionTableMapping();
		final MutatingTableReference tableReference = new MutatingTableReference( tableMapping );

		return new MutationOperationGroupSingle(
				MutationType.DELETE,
				getMutationTarget(),
				operationProducer.createOperation( tableReference )
		);
	}
}

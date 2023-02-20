/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinatorStandard;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.internal.MutationOperationGroupSingle;
import org.hibernate.sql.model.jdbc.JdbcMutationOperation;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_DEBUG_ENABLED;

public class ReactiveDeleteRowsCoordinatorStandard extends DeleteRowsCoordinatorStandard implements ReactiveDeleteRowsCoordinator {
	private final RowMutationOperations rowMutationOperations;
	private final boolean deleteByIndex;
	private MutationOperationGroupSingle operationGroup;
	private final BasicBatchKey batchKey;

	public ReactiveDeleteRowsCoordinatorStandard(CollectionMutationTarget mutationTarget, RowMutationOperations rowMutationOperations, boolean deleteByIndex) {
		super( mutationTarget, rowMutationOperations, deleteByIndex );
		this.deleteByIndex = deleteByIndex;
		this.rowMutationOperations = rowMutationOperations;
		this.batchKey = new BasicBatchKey( mutationTarget.getRolePath() + "#DELETE" );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		if ( operationGroup == null ) {
			operationGroup = createOperationGroup();
		}

		if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
			MODEL_MUTATION_LOGGER
					.debugf( "Deleting removed collection rows - %s : %s", getMutationTarget().getRolePath(), key );
		}

		final ReactiveMutationExecutor mutationExecutor = reactiveMutationExecutor( session, operationGroup );
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();

		return voidFuture()
				.thenCompose( unused -> {
					final PluralAttributeMapping pluralAttribute = getMutationTarget().getTargetPart();
					final CollectionPersister collectionDescriptor = pluralAttribute.getCollectionDescriptor();
					final Iterator<?> deletes = collection.getDeletes( collectionDescriptor, !deleteByIndex );
					if ( !deletes.hasNext() ) {
						MODEL_MUTATION_LOGGER.debug( "No rows to delete" );
						return voidFuture();
					}

					int[] deletionCount = { 0 };

					final RowMutationOperations.Restrictions restrictions = rowMutationOperations.getDeleteRowRestrictions();

					return loop( deletes, (removal, integer) -> {
						restrictions.applyRestrictions(
								collection,
								key,
								removal,
								deletionCount[0],
								session,
								jdbcValueBindings
						);

						return mutationExecutor.executeReactive( removal, null, null, null, session )
								.thenAccept( o -> deletionCount[0]++ );
					} ).thenAccept( ignore -> MODEL_MUTATION_LOGGER.debugf( "Done deleting `%s` collection rows : %s", deletionCount, getMutationTarget().getRolePath() ) );
				} )
				.whenComplete( (o, throwable) -> mutationExecutor.release() );
	}

	private ReactiveMutationExecutor reactiveMutationExecutor(SharedSessionContractImplementor session, MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		return  (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getBatchKey, operationGroup, session );
	}

	private MutationOperationGroupSingle createOperationGroup() {
		assert getMutationTarget().getTargetPart() != null;
		assert getMutationTarget().getTargetPart().getKeyDescriptor() != null;

		final JdbcMutationOperation operation = rowMutationOperations.getDeleteRowOperation();
		return new MutationOperationGroupSingle( MutationType.DELETE, getMutationTarget(), operation );
	}

	private BasicBatchKey getBatchKey() {
		return batchKey;
	}
}

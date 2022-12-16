/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.lang.invoke.MethodHandles;
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
import org.hibernate.persister.collection.mutation.InsertRowsCoordinatorStandard;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.internal.MutationOperationGroupSingle;
import org.hibernate.sql.model.jdbc.JdbcMutationOperation;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_DEBUG_ENABLED;

/**
 * @see InsertRowsCoordinatorStandard
 */
public class ReactiveInsertRowsCoordinatorStandard implements ReactiveInsertRowsCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final CollectionMutationTarget mutationTarget;
	private final RowMutationOperations rowMutationOperations;

	private final BasicBatchKey batchKey;

	private MutationOperationGroupSingle operationGroup;

	public ReactiveInsertRowsCoordinatorStandard(CollectionMutationTarget mutationTarget, RowMutationOperations rowMutationOperations) {
		this.mutationTarget = mutationTarget;
		this.rowMutationOperations = rowMutationOperations;
		this.batchKey = new BasicBatchKey( mutationTarget.getRolePath() + "#INSERT" );
	}

	@Override
	public CollectionMutationTarget getMutationTarget() {
		return mutationTarget;
	}

	@Override
	public void insertRows(PersistentCollection<?> collection, Object id, EntryFilter entryChecker, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveInsertRows" );
	}

	/**
	 * @see org.hibernate.persister.collection.mutation.InsertRowsCoordinator#insertRows(PersistentCollection, Object, EntryFilter, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveInsertRows(PersistentCollection<?> collection, Object id, EntryFilter entryChecker, SharedSessionContractImplementor session) {
		if ( operationGroup == null ) {
			operationGroup = createOperationGroup();
		}

		if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
			MODEL_MUTATION_LOGGER
					.debugf( "Inserting collection rows - %s : %s", mutationTarget.getRolePath(), id );
		}

		final PluralAttributeMapping pluralAttribute = mutationTarget.getTargetPart();
		final CollectionPersister collectionDescriptor = pluralAttribute.getCollectionDescriptor();
		final Iterator<?> entries = collection.entries( collectionDescriptor );

		collection.preInsert( collectionDescriptor );
		if ( !entries.hasNext() ) {
			MODEL_MUTATION_LOGGER.debugf( "No collection rows to insert - %s : %s", mutationTarget.getRolePath(), id );
			return voidFuture();
		}

		final ReactiveMutationExecutor mutationExecutor = reactiveMutationExecutor( session, operationGroup );
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();

		// It's just a counter, this way I don't have to use an Object but I can pass it to a lambda
		final int[] counter = { 0 };
		final RowMutationOperations.Values insertRowValues = rowMutationOperations.getInsertRowValues();
		return loop( entries, (entry, integer) -> {
					final int entryCount = counter[0];
					if ( entryChecker == null || entryChecker.include( entry, entryCount, collection, pluralAttribute ) ) {
						// if the entry is included, perform the "insert"
						insertRowValues.applyValues( collection, id, entry, entryCount, session, (value, jdbcValueMapping, usage) ->
							jdbcValueBindings.bindValue( value, jdbcValueMapping.getContainingTableExpression(), jdbcValueMapping.getSelectionExpression(), usage, session )
						);

						return mutationExecutor.executeReactive( entry, null, null, null, session );
					}
					counter[0]++;
					return voidFuture();
				} )
				.thenAccept( unused -> MODEL_MUTATION_LOGGER.debugf( "Done inserting `%s` collection rows : %s", counter[0], mutationTarget.getRolePath() ) )
				.whenComplete( (unused, throwable) -> mutationExecutor.release() );
	}

	private BasicBatchKey getBatchKey() {
		return batchKey;
	}

	private MutationOperationGroupSingle createOperationGroup() {
		assert mutationTarget.getTargetPart() != null;
		assert mutationTarget.getTargetPart().getKeyDescriptor() != null;

		final JdbcMutationOperation operation = rowMutationOperations.getInsertRowOperation();
		return new MutationOperationGroupSingle( MutationType.INSERT, mutationTarget, operation );
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

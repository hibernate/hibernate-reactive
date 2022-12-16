/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinatorStandard;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.MutationOperationGroup;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

/**
 * @see org.hibernate.persister.collection.mutation.UpdateRowsCoordinatorStandard
 */
public class ReactiveUpdateRowsCoordinatorStandard extends UpdateRowsCoordinatorStandard
		implements ReactiveUpdateRowsCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );
	private final RowMutationOperations rowMutationOperations;

	public ReactiveUpdateRowsCoordinatorStandard(CollectionMutationTarget mutationTarget, RowMutationOperations rowMutationOperations, SessionFactoryImplementor sessionFactory) {
		super( mutationTarget, rowMutationOperations, sessionFactory );
		this.rowMutationOperations = rowMutationOperations;
	}

	@Override
	public void updateRows(Object key, PersistentCollection<?> collection, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveUpdateRows" );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateRows(Object key, PersistentCollection<?> collection, SharedSessionContractImplementor session) {
		MODEL_MUTATION_LOGGER.tracef( "Updating collection rows - %s#%s", getMutationTarget().getRolePath(), key );

		// update all the modified entries
		return doReactiveUpdate( key, collection, session )
				.thenAccept( count -> MODEL_MUTATION_LOGGER
						.debugf(
								"Updated `%s` collection rows - %s#%s",
								count,
								getMutationTarget().getRolePath(),
								key
						) );
	}

	private CompletionStage<Integer> doReactiveUpdate(Object key, PersistentCollection<?> collection, SharedSessionContractImplementor session) {
		final ReactiveMutationExecutor mutationExecutor = reactiveMutationExecutor( session, getOperationGroup() );
		return completedFuture( mutationExecutor )
				.thenCompose( ignore -> {
					int[] counter = { 0 };
					final Iterator<?> entries = collection
							.entries( getMutationTarget().getTargetPart().getCollectionDescriptor() );

					if ( collection.isElementRemoved() ) {
						// the update should be done starting from the end of the elements
						// 		- make a copy so that we can go in reverse
						final List<Object> elements = new ArrayList<>();
						while ( entries.hasNext() ) {
							elements.add( entries.next() );
						}

						return loop( 0, elements.size(), index -> {
							final int i = elements.size() - index - 1;
							final Object entry = elements.get( i );
							return processRow( key, collection, entry, i, mutationExecutor, session )
									.thenAccept( updated -> {
										if ( updated ) {
											counter[0]++;
										}
									} );
						} ).thenApply( unused -> counter[0] );
					}
					else {
						final int[] position = { 0 };
						return loop( entries, (entry, integer) -> processRow(
								key,
								collection,
								entry,
								position[0]++,
								mutationExecutor,
								session
						).thenAccept( updated -> {
							if ( updated ) {
								counter[0]++;
							}
						} ) ).thenApply( unused -> counter[0] );
					}
				} )
				.whenComplete( (o, throwable) -> mutationExecutor.release() );
	}

	private CompletionStage<Boolean> processRow(
			Object key,
			PersistentCollection<?> collection,
			Object entry,
			int entryPosition,
			ReactiveMutationExecutor mutationExecutor,
			SharedSessionContractImplementor session) {
		final PluralAttributeMapping attribute = getMutationTarget().getTargetPart();
		if ( !collection.needsUpdating( entry, entryPosition, attribute ) ) {
			return falseFuture();
		}

		rowMutationOperations.getUpdateRowValues()
				.applyValues( collection, key, entry, entryPosition, session, (jdbcValue, jdbcValueMapping, usage) -> mutationExecutor.getJdbcValueBindings().bindValue(
						jdbcValue,
						jdbcValueMapping,
						usage,
						session
				)
		);

		rowMutationOperations.getUpdateRowRestrictions().applyRestrictions(
				collection,
				key,
				entry,
				entryPosition,
				session,
				(jdbcValue, jdbcValueMapping) -> mutationExecutor.getJdbcValueBindings().bindValue(
						jdbcValue,
						jdbcValueMapping,
						ParameterUsage.RESTRICT,
						session
				)
		);

		return mutationExecutor.executeReactive( collection, null, null, null, session )
				.thenCompose( ReactiveUpdateRowsCoordinatorStandard::alwaysTrue );
	}

	private static CompletionStage<Boolean> alwaysTrue(Object ignore) {
		return CompletionStages.trueFuture();
	}

	private ReactiveMutationExecutor reactiveMutationExecutor(
			SharedSessionContractImplementor session,
			MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getBatchKey, operationGroup, session );
	}

	private BatchKey getBatchKey() {
		return new BasicBatchKey( getMutationTarget().getRolePath() + "#UPDATE" );
	}
}

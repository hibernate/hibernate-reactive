/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.NullnessHelper;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.mapping.Collection;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoader;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderSubSelectFetch;
import org.hibernate.reactive.persister.collection.mutation.ReactiveDeleteRowsCoordinator;
import org.hibernate.reactive.persister.collection.mutation.ReactiveDeleteRowsCoordinatorNoOp;
import org.hibernate.reactive.persister.collection.mutation.ReactiveDeleteRowsCoordinatorStandard;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinator;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinatorNoOp;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinatorStandard;
import org.hibernate.reactive.persister.collection.mutation.ReactiveRemoveCoordinator;
import org.hibernate.reactive.persister.collection.mutation.ReactiveRemoveCoordinatorNoOp;
import org.hibernate.reactive.persister.collection.mutation.ReactiveRemoveCoordinatorStandard;
import org.hibernate.reactive.persister.collection.mutation.ReactiveUpdateRowsCoordinator;
import org.hibernate.reactive.persister.collection.mutation.ReactiveUpdateRowsCoordinatorNoOp;
import org.hibernate.reactive.persister.collection.mutation.ReactiveUpdateRowsCoordinatorOneToMany;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.internal.MutationOperationGroupSingle;
import org.hibernate.sql.model.jdbc.JdbcMutationOperation;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_DEBUG_ENABLED;

/**
 * A reactive {@link OneToManyPersister}
 */
public class ReactiveOneToManyPersister extends OneToManyPersister
		implements ReactiveAbstractCollectionPersister {

	private final ReactiveInsertRowsCoordinator insertRowsCoordinator;
	private final ReactiveUpdateRowsCoordinator updateRowsCoordinator;
	private final ReactiveDeleteRowsCoordinator deleteRowsCoordinator;
	private final ReactiveRemoveCoordinator removeCoordinator;

	public ReactiveOneToManyPersister(
			Collection collectionBinding,
			CollectionDataAccess cacheAccessStrategy,
			RuntimeModelCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );

		this.insertRowsCoordinator = buildInsertCoordinator();
		this.updateRowsCoordinator = buildUpdateCoordinator();
		this.deleteRowsCoordinator = buildDeleteCoordinator();
		this.removeCoordinator = buildDeleteAllCoordinator();
	}

	@Override
	public ReactiveInsertRowsCoordinator getInsertRowsCoordinator() {
		return insertRowsCoordinator;
	}

	@Override
	public ReactiveUpdateRowsCoordinator getUpdateRowsCoordinator() {
		return updateRowsCoordinator;
	}

	@Override
	public ReactiveDeleteRowsCoordinator getDeleteRowsCoordinator() {
		return deleteRowsCoordinator;
	}

	@Override
	public ReactiveRemoveCoordinator getRemoveCoordinator() {
		return removeCoordinator;
	}

	private ReactiveInsertRowsCoordinator buildInsertCoordinator() {
		if ( isInverse() || !isRowInsertEnabled() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection (re)creation - %s", getRolePath() );
			}
			return new ReactiveInsertRowsCoordinatorNoOp( this );
		}
		return new ReactiveInsertRowsCoordinatorStandard( this, getRowMutationOperations() );
	}

	private ReactiveUpdateRowsCoordinator buildUpdateCoordinator() {
		if ( !isRowDeleteEnabled() && !isRowInsertEnabled() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection row updates - %s", getRolePath() );
			}
			return new ReactiveUpdateRowsCoordinatorNoOp( this );
		}
		return new ReactiveUpdateRowsCoordinatorOneToMany( this, getRowMutationOperations(), getFactory() );
	}

	private ReactiveDeleteRowsCoordinator buildDeleteCoordinator() {
		if ( !needsRemove() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection row deletions - %s", getRolePath() );
			}
			return new ReactiveDeleteRowsCoordinatorNoOp( this );
		}
		// never delete by index for one-to-many
		return new ReactiveDeleteRowsCoordinatorStandard( this, getRowMutationOperations(), false );
	}

	private ReactiveRemoveCoordinator buildDeleteAllCoordinator() {
		if ( ! needsRemove() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection removals - %s", getRolePath() );
			}
			return new ReactiveRemoveCoordinatorNoOp( this );
		}
		return new ReactiveRemoveCoordinatorStandard( this, this::buildDeleteAllOperation );
	}

	private Parameters parameters() {
		return Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	@Override
	protected CollectionLoader createCollectionLoader(LoadQueryInfluencers loadQueryInfluencers) {
		return createReactiveCollectionLoader( loadQueryInfluencers );
	}

	@Override
	protected CollectionLoader createSubSelectLoader(SubselectFetch subselect, SharedSessionContractImplementor session) {
		return new ReactiveCollectionLoaderSubSelectFetch( getAttributeMapping(), null, subselect, session );
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Object key, SharedSessionContractImplementor session) {
		return ( (ReactiveCollectionLoader) determineLoaderToUse( key, session ) )
				.reactiveLoad( key, session )
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public boolean isRowDeleteEnabled() {
		return super.isRowDeleteEnabled();
	}

	@Override
	public boolean isRowInsertEnabled() {
		return super.isRowInsertEnabled();
	}

	@Override
	public boolean indexContainsFormula() {
		return super.indexContainsFormula;
	}

	private CompletionStage<Void> writeIndex(
			PersistentCollection<?> collection,
			Iterator<?> entries,
			Object key,
			boolean resetIndex,
			SharedSessionContractImplementor session) {
		if ( !entries.hasNext() ) {
			// no entries to update
			return voidFuture();
		}

		// If one-to-many and inverse, still need to create the index.  See HHH-5732.
		final boolean doWrite = isInverse && hasIndex() && !indexContainsFormula && ArrayHelper.countTrue( indexColumnIsSettable ) > 0;
		if ( !doWrite ) {
			return voidFuture();
		}

		final JdbcMutationOperation updateRowOperation = getRowMutationOperations().getUpdateRowOperation();
		final RowMutationOperations.Values updateRowValues = getRowMutationOperations().getUpdateRowValues();
		final RowMutationOperations.Restrictions updateRowRestrictions = getRowMutationOperations().getUpdateRowRestrictions();
		assert NullnessHelper.areAllNonNull( updateRowOperation, updateRowValues, updateRowRestrictions );

		final ReactiveMutationExecutor mutationExecutor = reactiveMutationExecutor( session, updateRowOperation );
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
		return voidFuture()
				.thenCompose( unused -> {
					final int[] nextIndex = { resetIndex ? 0 : getSize( key, session ) };
					return loop( entries, (entry, integer) -> {
						if ( entry != null && collection.entryExists( entry, nextIndex[0] ) ) {
							updateRowValues.applyValues( collection, key, entry, nextIndex[0], session, jdbcValueBindings );
							updateRowRestrictions.applyRestrictions( collection, key, entry, nextIndex[0], session, jdbcValueBindings );

							return mutationExecutor
									.executeReactive( collection, null, null, null, session )
									.thenAccept( o -> nextIndex[0]++ );
						}
						return voidFuture();
					} );
				} )
				.whenComplete( (o, throwable) -> mutationExecutor.release() );
	}

	private ReactiveMutationExecutor reactiveMutationExecutor(
			SharedSessionContractImplementor session,
			JdbcMutationOperation updateRowOperation) {
		final MutationExecutorService mutationExecutorService = getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );
		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor(
						this::getBasicBatchKey,
						new MutationOperationGroupSingle( MutationType.UPDATE, this, updateRowOperation ),
						session
				);
	}

	private BasicBatchKey getBasicBatchKey() {
		return new BasicBatchKey( getNavigableRole() + "#INDEX" );
	}

	/**
	 * @see OneToManyPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveRecreate(PersistentCollection collection, Object id, SharedSessionContractImplementor session) throws HibernateException {
		return getInsertRowsCoordinator()
				.reactiveInsertRows( collection, id, collection::includeInRecreate, session )
				.thenCompose( unused -> writeIndex( collection, collection.entries( this ), id, true, session ) );
	}

	/**
	 * @see OneToManyPersister#insertRows(PersistentCollection, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveInsertRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session) throws HibernateException {
		return getInsertRowsCoordinator()
				.reactiveInsertRows( collection, id, collection::includeInInsert, session )
				.thenCompose( unused -> writeIndex( collection, collection.entries( this ), id, true, session ) );
	}

	/**
	 * @see OneToManyPersister#updateRows(PersistentCollection, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveUpdateRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session) {
		return 	getUpdateRowsCoordinator().reactiveUpdateRows( id, collection, session );
	}


	/**
	 * @see OneToManyPersister#deleteRows(PersistentCollection, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session) {
		return getDeleteRowsCoordinator().reactiveDeleteRows(collection, id, session);
	}

	/**
	 * @see OneToManyPersister#remove(Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveRemove(Object id, SharedSessionContractImplementor session) {
		return getRemoveCoordinator().reactiveDeleteAllRows( id, session );
	}
}

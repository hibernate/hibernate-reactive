/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.mapping.Collection;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.collection.BasicCollectionPersister;
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
import org.hibernate.reactive.persister.collection.mutation.ReactiveUpdateRowsCoordinatorStandard;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_DEBUG_ENABLED;

/**
 * A reactive {@link BasicCollectionPersister}
 */
public class ReactiveBasicCollectionPersister extends BasicCollectionPersister implements ReactiveAbstractCollectionPersister {

	private final ReactiveInsertRowsCoordinator insertRowsCoordinator;
	private final ReactiveUpdateRowsCoordinator updateRowsCoordinator;
	private final ReactiveDeleteRowsCoordinator deleteRowsCoordinator;
	private final ReactiveRemoveCoordinator removeCoordinator;

	private Parameters parameters() {
		return Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	public ReactiveBasicCollectionPersister(
			Collection collectionBinding,
			CollectionDataAccess cacheAccessStrategy,
			RuntimeModelCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );

		this.insertRowsCoordinator = buildInsertRowCoordinator();
		this.updateRowsCoordinator = buildUpdateRowCoordinator();
		this.deleteRowsCoordinator = buildDeleteRowCoordinator();
		this.removeCoordinator = buildDeleteAllCoordinator();
	}


	private ReactiveUpdateRowsCoordinator buildUpdateRowCoordinator() {
		final boolean performUpdates = getCollectionSemantics().getCollectionClassification().isRowUpdatePossible()
				&& ArrayHelper.isAnyTrue( elementColumnIsSettable )
				&& !isInverse();

		if ( !performUpdates ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection row updates - %s", getRolePath() );
			}
			return new ReactiveUpdateRowsCoordinatorNoOp( this );
		}

		return new ReactiveUpdateRowsCoordinatorStandard( this, getRowMutationOperations(), getFactory() );
	}

	private ReactiveInsertRowsCoordinator buildInsertRowCoordinator() {
		if ( isInverse() || !isRowInsertEnabled() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection inserts - %s", getRolePath() );
			}
			return new ReactiveInsertRowsCoordinatorNoOp( this );
		}

		return new ReactiveInsertRowsCoordinatorStandard( this, getRowMutationOperations() );
	}

	private ReactiveDeleteRowsCoordinator buildDeleteRowCoordinator() {
		if ( ! needsRemove() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection row deletions - %s", getRolePath() );
			}
			return new ReactiveDeleteRowsCoordinatorNoOp( this );
		}

		return new ReactiveDeleteRowsCoordinatorStandard( this, getRowMutationOperations(), hasPhysicalIndexColumn() );
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

	/**
	 * @see org.hibernate.persister.collection.BasicCollectionPersister#remove(Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveRemove(Object id, SharedSessionContractImplementor session) {
		return getRemoveCoordinator().reactiveDeleteAllRows( id, session );
	}

	/**
	 * @see org.hibernate.persister.collection.BasicCollectionPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveRecreate(PersistentCollection collection, Object id, SharedSessionContractImplementor session) {
		return getCreateEntryCoordinator().reactiveInsertRows( collection, id, collection::includeInRecreate, session );
	}

	@Override
	public CompletionStage<Void> reactiveInsertRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session) {
		return getCreateEntryCoordinator().reactiveInsertRows( collection, id, collection::includeInInsert, session );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session) {
		return getUpdateEntryCoordinator().reactiveUpdateRows( id, collection, session );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session) {
		return getRemoveEntryCoordinator().reactiveDeleteRows( collection, id, session );
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

	@Override
	protected ReactiveInsertRowsCoordinator getCreateEntryCoordinator() {
		return insertRowsCoordinator;
	}

	@Override
	protected ReactiveUpdateRowsCoordinator getUpdateEntryCoordinator() {
		return updateRowsCoordinator;
	}

	public ReactiveInsertRowsCoordinator getInsertRowsCoordinator() {
		return insertRowsCoordinator;
	}

	@Override
	public ReactiveRemoveCoordinator getRemoveCoordinator() {
		return removeCoordinator;
	}

	@Override
	protected ReactiveDeleteRowsCoordinator getRemoveEntryCoordinator() {
		return deleteRowsCoordinator;
	}

	public ReactiveDeleteRowsCoordinator getDeleteRowsCoordinator() {
		return deleteRowsCoordinator;
	}

	public ReactiveUpdateRowsCoordinator getUpdateRowsCoordinator() {
		return updateRowsCoordinator;
	}
}

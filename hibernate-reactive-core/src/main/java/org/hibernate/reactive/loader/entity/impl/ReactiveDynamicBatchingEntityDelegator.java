/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.internal.BatchFetchQueueHelper;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.OuterJoinLoadable;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A batching entity loader for {@link org.hibernate.loader.BatchFetchStyle#DYNAMIC}
 * which selects between a single-key {@link ReactiveEntityLoader} and a batching
 * {@link ReactiveDynamicBatchingEntityLoader} depending upon how many keys it's given.
 *
 * @see org.hibernate.loader.entity.plan.DynamicBatchingEntityLoaderBuilder
 * @see org.hibernate.loader.entity.plan.DynamicBatchingEntityLoader
 */
public class ReactiveDynamicBatchingEntityDelegator extends ReactiveBatchingEntityLoader {

	private final int maxBatchSize;
	private final UniqueEntityLoader singleKeyLoader;
	private final ReactiveDynamicBatchingEntityLoader dynamicLoader;

	public ReactiveDynamicBatchingEntityDelegator(
			OuterJoinLoadable persister,
			int maxBatchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister );
		this.maxBatchSize = maxBatchSize;
		this.singleKeyLoader = new ReactiveEntityLoader( persister, 1, lockMode, factory, loadQueryInfluencers );
		this.dynamicLoader = new ReactiveDynamicBatchingEntityLoader( persister, maxBatchSize, lockMode, factory, loadQueryInfluencers );
	}

	public ReactiveDynamicBatchingEntityDelegator(
			OuterJoinLoadable persister,
			int maxBatchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister );
		this.maxBatchSize = maxBatchSize;
		this.singleKeyLoader = new ReactiveEntityLoader( persister, 1, lockOptions, factory, loadQueryInfluencers );
		this.dynamicLoader = new ReactiveDynamicBatchingEntityLoader( persister, maxBatchSize, lockOptions, factory, loadQueryInfluencers );
	}

	@Override
	public CompletionStage<Object> load(
			Serializable id,
			Object optionalObject,
			SharedSessionContractImplementor session,
			LockOptions lockOptions) {
		return load (id, optionalObject, session, lockOptions, null );
	}

	@Override
	public CompletionStage<Object> load(
			Serializable id,
			Object optionalObject,
			SharedSessionContractImplementor session,
			LockOptions lockOptions,
			Boolean readOnly) {
		final Serializable[] batch = session.getPersistenceContextInternal()
				.getBatchFetchQueue()
				.getEntityBatch( persister(), id, maxBatchSize, persister().getEntityMode() );

		final int numberOfIds = ArrayHelper.countNonNull( batch );
		if ( numberOfIds <= 1 ) {
			final Object result =  singleKeyLoader.load( id, optionalObject, session, lockOptions );
			if ( result == null ) {
				// There was no entity with the specified ID. Make sure the EntityKey does not remain
				// in the batch to avoid including it in future batches that get executed.
				BatchFetchQueueHelper.removeBatchLoadableEntityKey( id, persister(), session );
			}
			return completedFuture(result);
		}

		final Serializable[] idsToLoad = new Serializable[numberOfIds];
		System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );

//		if ( log.isDebugEnabled() ) {
//			log.debugf( "Batch loading entity: %s", MessageHelper.infoString( persister(), idsToLoad, session.getFactory() ) );
//		}

		QueryParameters qp = buildQueryParameters( id, idsToLoad, optionalObject, lockOptions, false );

		return dynamicLoader.doEntityBatchFetch( (SessionImplementor) session, qp, idsToLoad )
				.thenApply( results -> {
					// The EntityKey for any entity that is not found will remain in the batch.
					// Explicitly remove the EntityKeys for entities that were not found to
					// avoid including them in future batches that get executed.
					BatchFetchQueueHelper.removeNotFoundBatchLoadableEntityKeys( idsToLoad, results, persister(), session );

					return getObjectFromList(results, id, session);
				} );
	}
}

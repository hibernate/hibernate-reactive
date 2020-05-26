/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.persister.collection.QueryableCollection;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A batching entity loader for {@link org.hibernate.loader.BatchFetchStyle#DYNAMIC}
 * which selects between a single-key {@link ReactiveCollectionInitializer} and a
 * batching {@link ReactiveDynamicBatchingCollectionInitializer} depending upon how many keys it's
 * given.
 *
 * @see org.hibernate.loader.collection.DynamicBatchingCollectionInitializerBuilder.DynamicBatchingCollectionInitializer
 */
public class ReactiveDynamicBatchingCollectionDelegator extends ReactiveCollectionLoader {

	private final int maxBatchSize;
	private final ReactiveCollectionLoader singleKeyLoader;
	private final ReactiveDynamicBatchingCollectionInitializer batchLoader;

	public ReactiveDynamicBatchingCollectionDelegator(
			QueryableCollection collectionPersister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		super( collectionPersister, factory, influencers );
		this.maxBatchSize = maxBatchSize;

		if ( collectionPersister.isOneToMany() ) {
			this.singleKeyLoader = new ReactiveOneToManyLoader( collectionPersister, 1, factory, influencers );
		}
		else {
			throw new UnsupportedOperationException();
//				this.singleKeyLoader = new ReactiveBasicCollectionLoader( collectionPersister, 1, factory, influencers );
		}

		this.batchLoader = new ReactiveDynamicBatchingCollectionInitializer( collectionPersister, factory, influencers );
	}

	@Override
	public void initialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		// first, figure out how many batchable ids we have...
		final Serializable[] batch = session.getPersistenceContextInternal()
				.getBatchFetchQueue()
				.getCollectionBatch( collectionPersister(), id, maxBatchSize );
		final int numberOfIds = ArrayHelper.countNonNull( batch );
		if ( numberOfIds <= 1 ) {
			singleKeyLoader.loadCollection( session, id, collectionPersister().getKeyType() );
			return;
		}

		final Serializable[] idsToLoad = new Serializable[numberOfIds];
		System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );

		batchLoader.doBatchedCollectionLoad( (SessionImplementor) session, idsToLoad, collectionPersister().getKeyType() );
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session) {
		final Serializable[] batch = session.getPersistenceContextInternal()
				.getBatchFetchQueue()
				.getCollectionBatch( collectionPersister(), id, maxBatchSize );
		final int numberOfIds = ArrayHelper.countNonNull( batch );
		if ( numberOfIds <= 1 ) {
			return singleKeyLoader.reactiveLoadCollection( (SessionImplementor) session, id,
					collectionPersister().getKeyType() );
		}

		final Serializable[] idsToLoad = new Serializable[numberOfIds];
		System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );

		return batchLoader.doBatchedCollectionLoad( (SessionImplementor) session, idsToLoad,
				collectionPersister().getKeyType() );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.persister.collection.QueryableCollection;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;


/**
 * A batching collection initializer for {@link org.hibernate.loader.BatchFetchStyle#PADDED}.
 *
 * @see org.hibernate.loader.collection.PaddedBatchingCollectionInitializerBuilder.PaddedBatchingCollectionInitializer
 */
class ReactivePaddedBatchingCollectionInitializer extends ReactiveCollectionLoader {

	private final QueryableCollection persister;
	private final int[] batchSizes;
	private final ReactiveCollectionLoader[] loaders;

	public ReactivePaddedBatchingCollectionInitializer(
			QueryableCollection persister,
			int[] batchSizes,
			ReactiveCollectionLoader[] loaders,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister, factory, loadQueryInfluencers );
		this.persister = persister;
		this.batchSizes = batchSizes;
		this.loaders = loaders;
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session) {
		final Serializable[] batch = session.getPersistenceContextInternal()
				.getBatchFetchQueue()
				.getCollectionBatch( persister, id, batchSizes[0] );

		final int numberOfIds = ArrayHelper.countNonNull( batch );
		if ( numberOfIds <= 1 ) {
			return loaders[batchSizes.length - 1]
					.reactiveLoadCollection( (SharedSessionContractImplementor) session, id, persister.getKeyType() );
		}

		// Uses the first batch-size bigger than the number of actual ids in the batch
		int indexToUse = batchSizes.length - 1;
		for ( int i = 0; i < batchSizes.length - 1; i++ ) {
			if ( batchSizes[i] >= numberOfIds ) {
				indexToUse = i;
			}
			else {
				break;
			}
		}

		final Serializable[] idsToLoad = new Serializable[batchSizes[indexToUse]];
		System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );
		for ( int i = numberOfIds; i < batchSizes[indexToUse]; i++ ) {
			idsToLoad[i] = id;
		}

		return loaders[indexToUse].reactiveLoadCollectionBatch(
				(SessionImplementor) session,
				idsToLoad,
				persister.getKeyType()
		);
	}
}

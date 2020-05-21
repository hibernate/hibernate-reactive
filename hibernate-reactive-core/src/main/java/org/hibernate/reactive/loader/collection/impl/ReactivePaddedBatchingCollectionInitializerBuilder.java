/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A batch-fetch capable CollectionInitializer that performs batch-fetching using the padded style.  See
 * {@link org.hibernate.loader.BatchFetchStyle} for a discussion of the different styles.
 *
 * @author Gavin King
 *
 * @see org.hibernate.loader.BatchFetchStyle#PADDED
 */
public class ReactivePaddedBatchingCollectionInitializerBuilder extends ReactiveBatchingCollectionInitializerBuilder {
	public static final ReactivePaddedBatchingCollectionInitializerBuilder INSTANCE = new ReactivePaddedBatchingCollectionInitializerBuilder();

	@Override
	public ReactiveCollectionLoader createRealBatchingCollectionInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		int[] batchSizes = ArrayHelper.getBatchSizes( maxBatchSize );
		ReactiveCollectionLoader[] loaders = new ReactiveCollectionLoader[ batchSizes.length ];
		for ( int i = 0; i < batchSizes.length; i++ ) {
			throw new UnsupportedOperationException();
//			loaders[i] = new ReactiveBasicCollectionLoader( persister, batchSizes[i], factory, loadQueryInfluencers );
		}
		return new ReactivePaddedBatchingCollectionInitializer( persister, batchSizes, loaders, factory, loadQueryInfluencers);
	}

	@Override
	public ReactiveCollectionLoader createRealBatchingOneToManyInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		final int[] batchSizes = ArrayHelper.getBatchSizes( maxBatchSize );
		final ReactiveCollectionLoader[] loaders = new ReactiveCollectionLoader[ batchSizes.length ];
		for ( int i = 0; i < batchSizes.length; i++ ) {
			loaders[i] = new ReactiveOneToManyLoader( persister, batchSizes[i], factory, loadQueryInfluencers );
		}
		return new ReactivePaddedBatchingCollectionInitializer( persister, batchSizes, loaders, factory, loadQueryInfluencers);
	}

	private static class ReactivePaddedBatchingCollectionInitializer
			extends ReactiveCollectionLoader {
		private QueryableCollection persister;
		private final int[] batchSizes;
		private final ReactiveCollectionLoader[] loaders;

		public ReactivePaddedBatchingCollectionInitializer(QueryableCollection persister, int[] batchSizes,
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
				loaders[batchSizes.length-1].loadCollection( session, id, persister.getKeyType() );
				return CompletionStages.nullFuture();
			}

			// Uses the first batch-size bigger than the number of actual ids in the batch
			int indexToUse = batchSizes.length-1;
			for ( int i = 0; i < batchSizes.length-1; i++ ) {
				if ( batchSizes[i] >= numberOfIds ) {
					indexToUse = i;
				}
				else {
					break;
				}
			}

			final Serializable[] idsToLoad = new Serializable[ batchSizes[indexToUse] ];
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
}

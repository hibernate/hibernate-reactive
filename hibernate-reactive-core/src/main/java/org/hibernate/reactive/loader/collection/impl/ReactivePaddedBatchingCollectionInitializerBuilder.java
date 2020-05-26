/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.persister.collection.QueryableCollection;

/**
 * A {@link ReactiveBatchingCollectionInitializerBuilder} that is enabled when
 * {@link org.hibernate.loader.BatchFetchStyle#PADDED} is selected.
 *
 * A factory for {@link ReactivePaddedBatchingCollectionInitializer}s.
 *
 * @see org.hibernate.loader.collection.PaddedBatchingCollectionInitializerBuilder
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

}

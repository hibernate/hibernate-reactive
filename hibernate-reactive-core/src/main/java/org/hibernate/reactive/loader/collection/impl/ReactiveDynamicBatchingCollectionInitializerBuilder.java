/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.collection.QueryableCollection;


/**
 * A {@link ReactiveBatchingCollectionInitializerBuilder} that is enabled when
 * {@link org.hibernate.loader.BatchFetchStyle#DYNAMIC} is selected.
 *
 * A factory for {@link ReactiveDynamicBatchingCollectionDelegator}s.
 *
 * @see org.hibernate.loader.entity.DynamicBatchingEntityLoaderBuilder
 */
public class ReactiveDynamicBatchingCollectionInitializerBuilder extends ReactiveBatchingCollectionInitializerBuilder {

	public static final ReactiveDynamicBatchingCollectionInitializerBuilder INSTANCE = new ReactiveDynamicBatchingCollectionInitializerBuilder();

	@Override
	protected ReactiveCollectionLoader createRealBatchingCollectionInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveDynamicBatchingCollectionDelegator( persister, maxBatchSize, factory, influencers );
	}

	@Override
	protected ReactiveCollectionLoader createRealBatchingOneToManyInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveDynamicBatchingCollectionDelegator( persister, maxBatchSize, factory, influencers );
	}

}

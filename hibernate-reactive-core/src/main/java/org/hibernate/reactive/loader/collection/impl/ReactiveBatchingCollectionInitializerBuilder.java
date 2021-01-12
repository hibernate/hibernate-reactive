/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;

/**
 * Superclass of builders for batching
 * {@link ReactiveCollectionInitializer collection initializers}.
 *
 * The {@link #getBuilder(SessionFactoryImplementor)} method selects
 * between {@link ReactivePaddedBatchingCollectionInitializerBuilder} and
 * {@link ReactiveDynamicBatchingCollectionInitializerBuilder} depending upon
 * the {@link org.hibernate.loader.BatchFetchStyle} selected.
 *
 * @see org.hibernate.loader.entity.BatchingEntityLoaderBuilder
 */
public abstract class ReactiveBatchingCollectionInitializerBuilder {

	public static ReactiveBatchingCollectionInitializerBuilder getBuilder(SessionFactoryImplementor factory) {
		switch ( factory.getSessionFactoryOptions().getBatchFetchStyle() ) {
			case PADDED: {
				return ReactivePaddedBatchingCollectionInitializerBuilder.INSTANCE;
			}
			case DYNAMIC: {
				return ReactiveDynamicBatchingCollectionInitializerBuilder.INSTANCE;
			}
			default: {
				return ReactivePaddedBatchingCollectionInitializerBuilder.INSTANCE;
				//return LegacyBatchingCollectionInitializerBuilder.INSTANCE;
			}
		}
	}

	/**
	 * Builds a batch-fetch capable CollectionInitializer for basic and many-to-many collections (collections with
	 * a dedicated collection table).
	 *
	 * @param persister THe collection persister
	 * @param maxBatchSize The maximum number of keys to batch-fetch together
	 * @param factory The SessionFactory
	 * @param influencers Any influencers that should affect the built query
	 *
	 * @return The batch-fetch capable collection initializer
	 */
	public ReactiveCollectionLoader createBatchingCollectionInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		if ( maxBatchSize <= 1 ) {
			// no batching
			return buildNonBatchingLoader( persister, factory, influencers );
		}

		return createRealBatchingCollectionInitializer( persister, maxBatchSize, factory, influencers );
	}

	protected abstract ReactiveCollectionLoader createRealBatchingCollectionInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers);


	/**
	 * Builds a batch-fetch capable CollectionInitializer for one-to-many collections (collections without
	 * a dedicated collection table).
	 *
	 * @param persister THe collection persister
	 * @param maxBatchSize The maximum number of keys to batch-fetch together
	 * @param factory The SessionFactory
	 * @param influencers Any influencers that should affect the built query
	 *
	 * @return The batch-fetch capable collection initializer
	 */
	public ReactiveCollectionLoader createBatchingOneToManyInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		if ( maxBatchSize <= 1 ) {
			// no batching
			return buildNonBatchingLoader( persister, factory, influencers );
		}

		return createRealBatchingOneToManyInitializer( persister, maxBatchSize, factory, influencers );
	}

	protected abstract ReactiveCollectionLoader createRealBatchingOneToManyInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers);

	protected ReactiveCollectionLoader buildNonBatchingLoader(
			QueryableCollection persister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		if (persister.isOneToMany()) {
			return new ReactiveOneToManyLoader(persister, factory, influencers);
		}
		return new ReactiveBasicCollectionLoader(persister, factory, influencers);
	}
}

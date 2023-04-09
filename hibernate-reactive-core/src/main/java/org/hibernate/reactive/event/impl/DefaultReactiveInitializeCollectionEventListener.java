/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.entry.CollectionCacheEntry;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.CacheHelper;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.spi.InitializeCollectionEvent;
import org.hibernate.event.spi.InitializeCollectionEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.sql.results.internal.ResultsHelper;
import org.hibernate.stat.spi.StatisticsImplementor;

public class DefaultReactiveInitializeCollectionEventListener implements InitializeCollectionEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public void onInitializeCollection(InitializeCollectionEvent event) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "onReactiveInitializeCollection(InitializeCollectionEvent)" );
	}

	/**
	 * called by a collection that wants to initialize itself
	 */
	public CompletionStage<Void> onReactiveInitializeCollection(InitializeCollectionEvent event) throws HibernateException {
		final PersistentCollection<?> collection = event.getCollection();
		final SessionImplementor source = event.getSession();

		final CollectionEntry ce = source.getPersistenceContextInternal().getCollectionEntry( collection );
		if ( ce == null ) {
			throw LOG.collectionWasEvicted();
		}

		if ( collection.wasInitialized() ) {
			// Collection was already initialized.
			return voidFuture();
		}

		final CollectionPersister loadedPersister = ce.getLoadedPersister();
		final Object loadedKey = ce.getLoadedKey();
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Initializing collection {0}",
					collectionInfoString( loadedPersister, collection, loadedKey, source )
			);
			LOG.trace( "Checking second-level cache" );
		}

		final boolean foundInCache = initializeCollectionFromCache( loadedKey, loadedPersister, collection, source );
		if ( foundInCache ) {
			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Collection initialized from cache" );
			}
			return voidFuture();
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Collection not cached" );
			}
			return ( (ReactiveCollectionPersister) loadedPersister )
					.reactiveInitialize( loadedKey, source )
					.thenApply( list -> {
						handlePotentiallyEmptyCollection( collection, source, ce, loadedPersister );
						return list;
					} )
					.thenAccept( list -> {
						if ( LOG.isTraceEnabled() ) {
							LOG.trace( "Collection initialized" );
						}

						final StatisticsImplementor statistics = source.getFactory().getStatistics();
						if ( statistics.isStatisticsEnabled() ) {
							statistics.fetchCollection( loadedPersister.getRole() );
						}
					} );
		}
	}

	private void handlePotentiallyEmptyCollection(
			PersistentCollection<?> collection,
			SessionImplementor source,
			CollectionEntry ce,
			CollectionPersister loadedPersister) {
		if ( !collection.wasInitialized() ) {
			collection.initializeEmptyCollection( loadedPersister );
			ResultsHelper.finalizeCollectionLoading(
					source.getPersistenceContext(),
					loadedPersister,
					collection,
					ce.getLoadedKey(),
					true
			);
		}
	}

	/**
	 * Try to initialize a collection from the cache
	 *
	 * @param id The id of the collection to initialize
	 * @param persister The collection persister
	 * @param collection The collection to initialize
	 * @param source The originating session
	 *
	 * @return true if we were able to initialize the collection from the cache;
	 *         false otherwise.
	 */
	private boolean initializeCollectionFromCache(
			Object id,
			CollectionPersister persister,
			PersistentCollection<?> collection,
			SessionImplementor source) {

		if ( source.getLoadQueryInfluencers().hasEnabledFilters()
				&& persister.isAffectedByEnabledFilters( source ) ) {
			LOG.trace( "Disregarding cached version (if any) of collection due to enabled filters" );
			return false;
		}

		final boolean useCache = persister.hasCache() && source.getCacheMode().isGetEnabled();

		if ( !useCache ) {
			return false;
		}

		final SessionFactoryImplementor factory = source.getFactory();
		final CollectionDataAccess cacheAccessStrategy = persister.getCacheAccessStrategy();
		final Object ck = cacheAccessStrategy.generateCacheKey( id, persister, factory, source.getTenantIdentifier() );
		final Object ce = CacheHelper.fromSharedCache( source, ck, cacheAccessStrategy );

		final StatisticsImplementor statistics = factory.getStatistics();
		if ( statistics.isStatisticsEnabled() ) {
			if ( ce == null ) {
				statistics.collectionCacheMiss( persister.getNavigableRole(), cacheAccessStrategy.getRegion().getName() );
			}
			else {
				statistics.collectionCacheHit( persister.getNavigableRole(), cacheAccessStrategy.getRegion().getName() );
			}
		}

		if ( ce == null ) {
			return false;
		}

		final CollectionCacheEntry cacheEntry = (CollectionCacheEntry)
				persister.getCacheEntryStructure().destructure( ce, factory );

		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		cacheEntry.assemble( collection, persister, persistenceContext.getCollectionOwner( id, persister ) );
		persistenceContext.getCollectionEntry( collection ).postInitialize( collection );
		return true;
	}
}

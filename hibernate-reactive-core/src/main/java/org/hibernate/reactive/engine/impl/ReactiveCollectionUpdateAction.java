/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.CollectionAction;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PostCollectionUpdateEvent;
import org.hibernate.event.spi.PostCollectionUpdateEventListener;
import org.hibernate.event.spi.PreCollectionUpdateEvent;
import org.hibernate.event.spi.PreCollectionUpdateEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;


/**
 * Like {@link org.hibernate.action.internal.CollectionUpdateAction} but reactive
 *
 * @see org.hibernate.action.internal.CollectionUpdateAction
 */
public class ReactiveCollectionUpdateAction extends CollectionAction implements ReactiveExecutable {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final boolean emptySnapshot;

	public ReactiveCollectionUpdateAction(
			final PersistentCollection collection,
			final CollectionPersister persister,
			final Object key,
			final boolean emptySnapshot,
			final EventSource session) {
		super( persister, collection, key, session );
		this.emptySnapshot = emptySnapshot;
	}

	@Override
	public CompletionStage<Void> reactiveExecute() {
		final Object key = getKey();
		final SharedSessionContractImplementor session = getSession();
		final ReactiveCollectionPersister reactivePersister = (ReactiveCollectionPersister) getPersister();
		final CollectionPersister persister = getPersister();
		final PersistentCollection<?> collection = getCollection();
		final boolean affectedByFilters = persister.isAffectedByEnabledFilters( session );

		preUpdate();

		return createUpdateStage( collection, affectedByFilters, reactivePersister, key, session, persister )
				.thenRun( () -> {
					session.getPersistenceContextInternal().getCollectionEntry( collection ).afterAction( collection );
					evict();
					postUpdate();

					final StatisticsImplementor statistics = session.getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.updateCollection( persister.getRole() );
					}
				} );
	}

	private CompletionStage<Void> createUpdateStage(
			PersistentCollection<?> collection,
			boolean affectedByFilters,
			ReactiveCollectionPersister reactivePersister,
			Object key,
			SharedSessionContractImplementor session,
			CollectionPersister persister) {
		if ( !collection.wasInitialized() ) {
			// If there were queued operations, they would have been processed
			// and cleared by now.
			// The collection should still be dirty.
			if ( !collection.isDirty() ) {
				return failedFuture( new AssertionFailure( "collection is not dirty" ) );
			}
			//do nothing - we only need to notify the cache...
			return voidFuture();
		}
		else if ( !affectedByFilters && collection.empty() ) {
			return emptySnapshot ? voidFuture() : reactivePersister.reactiveRemove( key, session );
		}
		else if ( collection.needsRecreate( persister ) ) {
			if ( affectedByFilters ) {
				return failedFuture( LOG.cannotRecreateCollectionWhileFilterIsEnabled( collectionInfoString( persister, collection, key, session ) ) );
			}
			return emptySnapshot
					? reactivePersister.reactiveRecreate( collection, key, session )
					: reactivePersister.reactiveRemove( key, session )
							.thenCompose( v -> reactivePersister.reactiveRecreate( collection, key, session ) );
		}
		else {
			return reactivePersister
					.reactiveDeleteRows( collection, key, session )
					.thenCompose( v -> reactivePersister.reactiveUpdateRows( collection, key, session ) )
					.thenCompose( v -> reactivePersister.reactiveInsertRows( collection, key, session ) );
		}
	}

	@Override
	public void execute() throws HibernateException {
		// Unsupported in reactive see reactiveExecute()
		throw new UnsupportedOperationException( "Use reactiveExecute() instead" );
	}

	private void preUpdate() {
		getFastSessionServices().eventListenerGroup_PRE_COLLECTION_UPDATE
				.fireLazyEventOnEachListener( this::newPreCollectionUpdateEvent,
						PreCollectionUpdateEventListener::onPreUpdateCollection );
	}

	private PreCollectionUpdateEvent newPreCollectionUpdateEvent() {
		return new PreCollectionUpdateEvent(
				getPersister(),
				getCollection(),
				eventSource()
		);
	}

	private void postUpdate() {
		getFastSessionServices().eventListenerGroup_POST_COLLECTION_UPDATE
				.fireLazyEventOnEachListener( this::newPostCollectionUpdateEvent,
						PostCollectionUpdateEventListener::onPostUpdateCollection );
	}

	private PostCollectionUpdateEvent newPostCollectionUpdateEvent() {
		return new PostCollectionUpdateEvent(
				getPersister(),
				getCollection(),
				eventSource()
		);
	}

}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.CollectionAction;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PostCollectionRemoveEvent;
import org.hibernate.event.spi.PostCollectionRemoveEventListener;
import org.hibernate.event.spi.PreCollectionRemoveEvent;
import org.hibernate.event.spi.PreCollectionRemoveEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveCollectionRemoveAction extends CollectionAction implements ReactiveExecutable {
	private final Object affectedOwner;
	private final boolean emptySnapshot;

	public ReactiveCollectionRemoveAction(
			final PersistentCollection collection,
			final CollectionPersister persister,
			final Object key,
			final boolean emptySnapshot,
			final EventSource session) {
		super( persister, collection, key, session );
		if ( collection == null ) {
			throw new AssertionFailure( "collection == null");
		}
		this.emptySnapshot = emptySnapshot;
		// the loaded owner will be set to null after the collection is removed,
		// so capture its value as the affected owner so it is accessible to
		// both pre- and post- events
		this.affectedOwner = session.getPersistenceContextInternal().getLoadedCollectionOwnerOrNull( collection );
	}

	/**
	 * Removes a persistent collection for an unloaded proxy.
	 *
	 * Use this constructor when the owning entity is has not been loaded.
	 * @param persister The collection's persister
	 * @param id The collection key
	 * @param session The session
	 */
	public ReactiveCollectionRemoveAction(
			final CollectionPersister persister,
			final Object id,
			final EventSource session) {
		super( persister, null, id, session );
		emptySnapshot = false;
		affectedOwner = null;
	}

	@Override
	public CompletionStage<Void> reactiveExecute() {
		preRemove();

		final SharedSessionContractImplementor session = getSession();
		CompletionStage<Void> removeStage;
		if ( emptySnapshot ) {
			removeStage = voidFuture();
		}
		else {
			final ReactiveCollectionPersister reactivePersister = (ReactiveCollectionPersister) getPersister();
			// an existing collection that was either non-empty or uninitialized
			// is replaced by null or a different collection
			// (if the collection is uninitialized, hibernate has no way of
			// knowing if the collection is actually empty without querying the db)
			removeStage = reactivePersister.reactiveRemove( getKey(), session );
		}
		return removeStage.thenAccept( v -> {
			final PersistentCollection<?> collection = getCollection();
			if ( collection != null ) {
				session.getPersistenceContextInternal().getCollectionEntry( collection ).afterAction( collection );
			}
			evict();
			postRemove();
			final StatisticsImplementor statistics = session.getFactory().getStatistics();
			if ( statistics.isStatisticsEnabled() ) {
				statistics.updateCollection( getPersister().getRole() );
			}
		} );
	}

	@Override
	public void execute() throws HibernateException {
		// Unsupported in reactive see reactiveExecute()
		throw new UnsupportedOperationException( "Use reactiveExecute() instead" );
	}

	private void preRemove() {
		getFastSessionServices().eventListenerGroup_PRE_COLLECTION_REMOVE
				.fireLazyEventOnEachListener( this::newPreCollectionRemoveEvent,
						PreCollectionRemoveEventListener::onPreRemoveCollection );
	}

	private PreCollectionRemoveEvent newPreCollectionRemoveEvent() {
		return new PreCollectionRemoveEvent(
				getPersister(),
				getCollection(),
				eventSource(),
				affectedOwner
		);
	}

	private void postRemove() {
		getFastSessionServices().eventListenerGroup_POST_COLLECTION_REMOVE
				.fireLazyEventOnEachListener( this::newPostCollectionRemoveEvent,
						PostCollectionRemoveEventListener::onPostRemoveCollection );
	}

	private PostCollectionRemoveEvent newPostCollectionRemoveEvent() {
		return new PostCollectionRemoveEvent(
				getPersister(),
				getCollection(),
				eventSource(),
				affectedOwner
		);
	}
}

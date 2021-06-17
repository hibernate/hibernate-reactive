/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.CollectionAction;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostCollectionRecreateEvent;
import org.hibernate.event.spi.PostCollectionRecreateEventListener;
import org.hibernate.event.spi.PostCollectionRemoveEvent;
import org.hibernate.event.spi.PostCollectionRemoveEventListener;
import org.hibernate.event.spi.PreCollectionRecreateEvent;
import org.hibernate.event.spi.PreCollectionRecreateEventListener;
import org.hibernate.event.spi.PreCollectionRemoveEvent;
import org.hibernate.event.spi.PreCollectionRemoveEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.spi.StatisticsImplementor;

public class ReactiveCollectionRemoveAction extends CollectionAction implements ReactiveExecutable {
	private final Object affectedOwner;
	private final boolean emptySnapshot;

	public ReactiveCollectionRemoveAction(
			final PersistentCollection collection,
			final CollectionPersister persister,
			final Serializable key,
			final boolean emptySnapshot,
			final SharedSessionContractImplementor session) {
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

	@Override
	public CompletionStage<Void> reactiveExecute() {
		final Serializable key = getKey();
		final SharedSessionContractImplementor session = getSession();
		final ReactiveCollectionPersister reactivePersister = (ReactiveCollectionPersister) getPersister();
		final CollectionPersister corePersister = getPersister();
		final PersistentCollection collection = getCollection();
		final StatisticsImplementor statistics = session.getFactory().getStatistics();

		CompletionStage<Void> removeStage = CompletionStages.voidFuture();

		if ( !emptySnapshot ) {
			// an existing collection that was either non-empty or uninitialized
			// is replaced by null or a different collection
			// (if the collection is uninitialized, hibernate has no way of
			// knowing if the collection is actually empty without querying the db)
			removeStage = removeStage.thenAccept( v -> preRemove() )
					.thenCompose( v -> reactivePersister
							.removeReactive( key, session )
							.thenAccept( ignore -> {
								evict();
								postRemove();
							})
					);
		}
		if( collection != null ) {
			return removeStage.thenAccept(v -> {
				session.getPersistenceContextInternal().getCollectionEntry( collection ).afterAction( collection );
				evict();
				postRemove();
				if ( statistics.isStatisticsEnabled() ) {
					statistics.updateCollection( corePersister.getRole() );
				}
			} );
		}
		return removeStage.thenAccept(v -> {
			evict();
			postRemove();
			if ( statistics.isStatisticsEnabled() ) {
				statistics.updateCollection( corePersister.getRole() );
			}
		} );

	}

	@Override
	public void execute() throws HibernateException {
		// Unsupported in reactive see reactiveExecute()
		throw new UnsupportedOperationException( "Use reactiveExecute() instead" );
	}

	private void preRemove() {
		final EventListenerGroup<PreCollectionRemoveEventListener> listenerGroup = listenerGroup( EventType.PRE_COLLECTION_REMOVE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PreCollectionRemoveEvent event = new PreCollectionRemoveEvent(
				getPersister(),
				getCollection(),
				eventSource(),
				affectedOwner
		);
		for ( PreCollectionRemoveEventListener listener : listenerGroup.listeners() ) {
			listener.onPreRemoveCollection( event );
		}
	}

	private void postRemove() {
		final EventListenerGroup<PostCollectionRemoveEventListener> listenerGroup = listenerGroup( EventType.POST_COLLECTION_REMOVE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PostCollectionRemoveEvent event = new PostCollectionRemoveEvent(
				getPersister(),
				getCollection(),
				eventSource(),
				affectedOwner
		);
		for ( PostCollectionRemoveEventListener listener : listenerGroup.listeners() ) {
			listener.onPostRemoveCollection( event );
		}
	}

	private void preRecreate() {
		final EventListenerGroup<PreCollectionRecreateEventListener> listenerGroup = listenerGroup( EventType.PRE_COLLECTION_RECREATE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PreCollectionRecreateEvent event = new PreCollectionRecreateEvent( getPersister(), getCollection(), eventSource() );
		for ( PreCollectionRecreateEventListener listener : listenerGroup.listeners() ) {
			listener.onPreRecreateCollection( event );
		}
	}

	private void postRecreate() {
		final EventListenerGroup<PostCollectionRecreateEventListener> listenerGroup = listenerGroup( EventType.POST_COLLECTION_RECREATE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PostCollectionRecreateEvent event = new PostCollectionRecreateEvent( getPersister(), getCollection(), eventSource() );
		for ( PostCollectionRecreateEventListener listener : listenerGroup.listeners() ) {
			listener.onPostRecreateCollection( event );
		}
	}

}

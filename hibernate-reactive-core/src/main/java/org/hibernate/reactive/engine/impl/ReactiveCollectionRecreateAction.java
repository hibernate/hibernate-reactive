/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.CollectionAction;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostCollectionRecreateEvent;
import org.hibernate.event.spi.PostCollectionRecreateEventListener;
import org.hibernate.event.spi.PreCollectionRecreateEvent;
import org.hibernate.event.spi.PreCollectionRecreateEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;

public class ReactiveCollectionRecreateAction extends CollectionAction implements ReactiveExecutable {

	public ReactiveCollectionRecreateAction(
			final PersistentCollection collection,
			final CollectionPersister persister,
			final Serializable key,
			final SharedSessionContractImplementor session) {
		super( persister, collection, key, session );
	}

	@Override
	public CompletionStage<Void> reactiveExecute() {
		final ReactiveCollectionPersister persister = (ReactiveCollectionPersister)getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Serializable key = getKey();

		final PersistentCollection collection = getCollection();

		preRecreate();

		return persister.recreateReactive( collection, key, session ).thenAccept( v -> {
			session.getPersistenceContextInternal().getCollectionEntry( collection ).afterAction( collection );
			evict();
			postRecreate();
		} );
	}

	@Override
	public void execute() throws HibernateException {
		// Unsupported in reactive see reactiveExecute()
		throw new UnsupportedOperationException( "Use reactiveExecute() instead" );
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

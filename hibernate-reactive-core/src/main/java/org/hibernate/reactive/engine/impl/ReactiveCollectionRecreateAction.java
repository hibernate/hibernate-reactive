/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.CollectionAction;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PostCollectionRecreateEvent;
import org.hibernate.event.spi.PostCollectionRecreateEventListener;
import org.hibernate.event.spi.PreCollectionRecreateEvent;
import org.hibernate.event.spi.PreCollectionRecreateEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

public class ReactiveCollectionRecreateAction extends CollectionAction implements ReactiveExecutable {

	public ReactiveCollectionRecreateAction(final PersistentCollection collection, final CollectionPersister persister, final Object key, final EventSource session) {
		super( persister, collection, key, session );
	}

	@Override
	public CompletionStage<Void> reactiveExecute() {
		final ReactiveCollectionPersister persister = (ReactiveCollectionPersister) getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object key = getKey();

		final PersistentCollection<?> collection = getCollection();

		preRecreate();

		return persister
				.reactiveRecreate( collection, key, session )
				.thenAccept( v -> {
					// FIXME: I think we could move everything in a method reference call
					session.getPersistenceContextInternal().getCollectionEntry( collection ).afterAction( collection );
					evict();
					postRecreate();
					final StatisticsImplementor statistics = session.getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.recreateCollection( getPersister().getRole() );
					}
				} );
	}

	@Override
	public void execute() throws HibernateException {
		// Unsupported in reactive see reactiveExecute()
		throw new UnsupportedOperationException( "Use reactiveExecute() instead" );
	}

	private void preRecreate() {
		getFastSessionServices()
				.eventListenerGroup_PRE_COLLECTION_RECREATE
				.fireLazyEventOnEachListener( this::newPreCollectionRecreateEvent, PreCollectionRecreateEventListener::onPreRecreateCollection );
	}

	private PreCollectionRecreateEvent newPreCollectionRecreateEvent() {
		return new PreCollectionRecreateEvent( getPersister(), getCollection(), eventSource() );
	}

	private void postRecreate() {
		getFastSessionServices()
				.eventListenerGroup_POST_COLLECTION_RECREATE
				.fireLazyEventOnEachListener( this::newPostCollectionRecreateEvent, PostCollectionRecreateEventListener::onPostRecreateCollection );
	}

	private PostCollectionRecreateEvent newPostCollectionRecreateEvent() {
		return new PostCollectionRecreateEvent( getPersister(), getCollection(), eventSource() );
	}
}

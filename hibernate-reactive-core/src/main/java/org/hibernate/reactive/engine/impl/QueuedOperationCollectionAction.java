/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.CollectionAction;
import org.hibernate.collection.spi.AbstractPersistentCollection;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Copy of {@link  QueuedOperationCollectionAction}, that adapts to {@link ReactiveExecutable}
 *
 * @author Gavin King
 */
public final class QueuedOperationCollectionAction extends CollectionAction implements ReactiveExecutable {

	/**
	 * Constructs a CollectionUpdateAction
	 *
	 * @param collection The collection to update
	 * @param persister The collection persister
	 * @param id The collection key
	 * @param session The session
	 */
	public QueuedOperationCollectionAction(final PersistentCollection collection, final CollectionPersister persister, final Object id, final EventSource session) {
		super( persister, collection, id, session );
	}

	@Override
	public CompletionStage<Void> reactiveExecute() {
		// this QueuedOperationCollectionAction has to be executed before any other
		// CollectionAction involving the same collection.

		// this is not needed in HR, unless we someday add support for extra-lazy collections
//		getPersister().processQueuedOps( getCollection(), getKey(), getSession() );

		// TODO: It would be nice if this could be done safely by CollectionPersister#processQueuedOps;
		//       Can't change the SPI to do this though.
		( (AbstractPersistentCollection<?>) getCollection() ).clearOperationQueue();

		// The other CollectionAction types call CollectionEntry#afterAction, which
		// clears the dirty flag. We don't want to call CollectionEntry#afterAction unless
		// there is no other CollectionAction that will be executed on the same collection.
		final CollectionEntry ce = getSession().getPersistenceContextInternal().getCollectionEntry( getCollection() );
		if ( !ce.isDoremove() && !ce.isDoupdate() && !ce.isDorecreate() ) {
			ce.afterAction( getCollection() );
		}
		return voidFuture();
	}

	@Override
	public void execute() throws HibernateException {
		throw new UnsupportedOperationException();
	}
}

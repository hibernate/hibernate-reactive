/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.ReactiveSession;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Add reactive methods to a {@link PersistenceContext}.
 */
public class ReactivePersistenceContextAdapter extends StatefulPersistenceContext {

	private HashMap<Serializable, Object[]> entitySnapshotsByKey;

	/**
	 * Constructs a PersistentContext, bound to the given session.
	 *
	 * @param session The session "owning" this context.
	 */
	public ReactivePersistenceContextAdapter(SharedSessionContractImplementor session) {
		super( session );
	}

	public CompletionStage<Void> reactiveInitializeNonLazyCollections() throws HibernateException {
		final NonLazyCollectionInitializer initializer = new NonLazyCollectionInitializer();
		initializeNonLazyCollections( initializer );
		return initializer.stage;
	}

	private class NonLazyCollectionInitializer implements Consumer<PersistentCollection<?>> {
		CompletionStage<Void> stage = voidFuture();

		@Override
		public void accept(PersistentCollection<?> nonLazyCollection) {
			if ( !nonLazyCollection.wasInitialized() ) {
				stage = stage.thenCompose( v -> ( (ReactiveSession) getSession() )
						.reactiveInitializeCollection( nonLazyCollection, false ) );
			}
		}
	}

	/**
	 * @deprecated use {@link #reactiveInitializeNonLazyCollections} instead.
	 */
	@Deprecated
	@Override
	public void initializeNonLazyCollections() {
		// still called by ResultSetProcessorImpl, so can't throw UnsupportedOperationException
	}

	@Deprecated
	@Override
	public Object[] getDatabaseSnapshot(Object id, EntityPersister persister) throws HibernateException {
		throw new UnsupportedOperationException( "reactive persistence context" );
	}

	private static final Object[] NO_ROW = new Object[]{ StatefulPersistenceContext.NO_ROW };

	public CompletionStage<Object[]> reactiveGetDatabaseSnapshot(Object id, EntityPersister persister)
			throws HibernateException {

		SessionImplementor session = (SessionImplementor) getSession();
		final EntityKey key = session.generateEntityKey( id, persister );
		final Object[] cached = entitySnapshotsByKey == null ? null : entitySnapshotsByKey.get( key );
		if ( cached != null ) {
			return completedFuture( cached == NO_ROW ? null : cached );
		}
		else {
			return ( (ReactiveEntityPersister) persister )
					.reactiveGetDatabaseSnapshot( id, session )
					.thenApply( snapshot -> {
						if ( entitySnapshotsByKey == null ) {
							entitySnapshotsByKey = new HashMap<>( 8 );
						}
						entitySnapshotsByKey.put( key, snapshot == null ? NO_ROW : snapshot );
						return snapshot;
					} );
		}
	}

	//All below methods copy/pasted from superclass because entitySnapshotsByKey is private:

	@Override
	public Object[] getCachedDatabaseSnapshot(EntityKey key) {
		final Object[] snapshot = entitySnapshotsByKey == null ? null : entitySnapshotsByKey.get( key );
		if ( snapshot == NO_ROW ) {
			throw new IllegalStateException(
					"persistence context reported no row snapshot for "
							+ infoString( key.getEntityName(), key.getIdentifier() )
			);
		}
		return snapshot;
	}

	@Override
	public void clear() {
		super.clear();
		entitySnapshotsByKey = null;
	}

	@Override
	public Object removeEntity(EntityKey key) {
		Object result = super.removeEntity(key);
		if (entitySnapshotsByKey != null ) {
			entitySnapshotsByKey.remove(key);
		}
		return result;
	}
}

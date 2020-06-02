/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.ObjectDeletedException;
import org.hibernate.TransientObjectException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.AbstractReassociateEventListener;
import org.hibernate.event.internal.DefaultLockEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LockEvent;
import org.hibernate.event.spi.LockEventListener;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingActions;
import org.hibernate.reactive.engine.impl.ForeignKeys;
import org.hibernate.reactive.event.ReactiveLockEventListener;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class DefaultReactiveLockEventListener extends AbstractReassociateEventListener
		implements LockEventListener, ReactiveLockEventListener {

	private static final CoreMessageLogger log = Logger.getMessageLogger(
			CoreMessageLogger.class,
			DefaultLockEventListener.class.getName()
	);

	@Override
	public CompletionStage<Void> reactiveOnLock(LockEvent event) throws HibernateException {
		if ( event.getObject() == null ) {
			throw new NullPointerException( "attempted to lock null" );
		}

		if ( event.getLockMode() == LockMode.WRITE ) {
			throw new HibernateException( "Invalid lock mode for lock()" );
		}

		if ( event.getLockMode() == LockMode.UPGRADE_SKIPLOCKED ) {
			log.explicitSkipLockedLockCombo();
		}

		SessionImplementor source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );
		//TODO: if object was an uninitialized proxy, this is inefficient,
		//      resulting in two SQL selects

		EntityEntry entry = persistenceContext.getEntry(entity);
		CompletionStage<EntityEntry> stage;
		if (entry==null) {
			final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );
			final Serializable id = persister.getIdentifier( entity, source );
			stage = ForeignKeys.isNotTransient( event.getEntityName(), entity, Boolean.FALSE, source )
					.thenApply(
							trans -> {
								if (!trans) {
									throw new TransientObjectException(
											"cannot lock an unsaved transient instance: " +
													persister.getEntityName()
									);
								}

								EntityEntry e = reassociate(event, entity, id, persister);
								cascadeOnLock(event, persister, entity);
								return e;
							} );

		}
		else {
			stage = CompletionStages.completedFuture(entry);
		}

		return stage.thenCompose( e -> upgradeLock( entity, e, event.getLockOptions(), event.getSession() ) );
	}

	private void cascadeOnLock(LockEvent event, EntityPersister persister, Object entity) {
		EventSource source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		persistenceContext.incrementCascadeLevel();
		try {
			new Cascade(
					CascadingActions.LOCK,
					CascadePoint.AFTER_LOCK,
					persister,
					entity,
					event.getLockOptions(),
					source
			).cascade();
		}
		finally {
			persistenceContext.decrementCascadeLevel();
		}
	}

	/**
	 * Performs a pessimistic lock upgrade on a given entity, if needed.
	 *
	 * @param object The entity for which to upgrade the lock.
	 * @param entry The entity's EntityEntry instance.
	 * @param lockOptions contains the requested lock mode.
	 * @param source The session which is the source of the event being processed.
	 */
	protected CompletionStage<Void> upgradeLock(Object object, EntityEntry entry,
												LockOptions lockOptions,
												EventSource source) {

		LockMode requestedLockMode = lockOptions.getLockMode();
		if ( requestedLockMode.greaterThan( entry.getLockMode() ) ) {
			// The user requested a "greater" (i.e. more restrictive) form of
			// pessimistic lock

			if ( entry.getStatus() != Status.MANAGED ) {
				throw new ObjectDeletedException(
						"attempted to lock a deleted instance",
						entry.getId(),
						entry.getPersister().getEntityName()
				);
			}

			final EntityPersister persister = entry.getPersister();

			if ( log.isTraceEnabled() ) {
				log.tracev(
						"Locking {0} in mode: {1}",
						MessageHelper.infoString( persister, entry.getId(), source.getFactory() ),
						requestedLockMode
				);
			}

			final boolean cachingEnabled = persister.canWriteToCache();
			final SoftLock lock;
			final Object ck;
			if ( cachingEnabled ) {
				EntityDataAccess cache = persister.getCacheAccessStrategy();
				ck = cache.generateCacheKey(
						entry.getId(),
						persister,
						source.getFactory(),
						source.getTenantIdentifier()
				);
				lock = cache.lockItem( source, ck, entry.getVersion() );
			}
			else {
				lock = null;
				ck = null;
			}

			return ((ReactiveEntityPersister) persister).lockReactive(
					entry.getId(),
					entry.getVersion(),
					object,
					lockOptions,
					source
			).thenAccept( v -> entry.setLockMode(requestedLockMode) )
					.whenComplete( (r, e) -> {
						// the database now holds a lock + the object is flushed from the cache,
						// so release the soft lock
						if ( cachingEnabled ) {
							persister.getCacheAccessStrategy().unlockItem( source, ck, lock );
						}
					} );

		}
		else {
			return CompletionStages.nullFuture();
		}
	}

	@Override
	public void onLock(LockEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}
}

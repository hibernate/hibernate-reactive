/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.DetachedObjectException;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.ObjectDeletedException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.DefaultLockEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LockEvent;
import org.hibernate.event.spi.LockEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.ReactiveEntityIncrementVersionProcess;
import org.hibernate.reactive.engine.impl.ReactiveEntityVerifyVersionProcess;
import org.hibernate.reactive.event.ReactiveLockEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.ReactiveQueryProducer;
import org.hibernate.reactive.session.ReactiveSession;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class DefaultReactiveLockEventListener extends DefaultLockEventListener implements LockEventListener, ReactiveLockEventListener {

	private static final Log LOG = make( Log.class, lookup() );

	@Override
	public void onLock(LockEvent event) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveOnLock" );
	}

	@Override
	public CompletionStage<Void> reactiveOnLock(LockEvent event) throws HibernateException {
		final Object instance = event.getObject();
		if ( instance == null ) {
			throw new NullPointerException( "attempted to lock null" );
		}

		final var lockMode = event.getLockMode();
		if ( lockMode == LockMode.WRITE ) {
			throw LOG.invalidLockModeForLock();
		}

		if ( lockMode == LockMode.UPGRADE_SKIPLOCKED ) {
			LOG.explicitSkipLockedLockCombo();
		}

		final EventSource source = event.getSession();

		boolean detached = event.getEntityName() != null
				? !source.contains( event.getEntityName(), event.getObject() )
				: !source.contains( event.getObject() );
		if ( detached ) {
			// Hibernate Reactive doesn't support detached instances in refresh()
			throw new IllegalArgumentException( "Unmanaged instance passed to refresh()" );
		}


//		Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );
		//TODO: if object was an uninitialized proxy, this is inefficient,
		//      resulting in two SQL selects

		return ( (ReactiveQueryProducer) source ).reactiveFetch( instance, true )
				.thenCompose( entity -> reactiveOnLock( event, entity ) );
	}

	private CompletionStage<Void> reactiveOnLock(LockEvent event, Object entity) {
		final SessionImplementor source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		final EntityEntry entry = persistenceContext.getEntry( entity );
		if ( entry == null && event.getObject() == entity ) {
			throw new DetachedObjectException( "Given entity is not associated with the persistence context" );
		}
		return upgradeLock( entity, entry, event.getLockOptions(), event.getSession() );
	}


	/**
	 * Performs a pessimistic lock upgrade on a given entity, if needed.
	 *
	 * @param object      The entity for which to upgrade the lock.
	 * @param entry       The entity's EntityEntry instance.
	 * @param lockOptions contains the requested lock mode.
	 * @param source      The session which is the source of the event being processed.
	 */
	protected CompletionStage<Void> upgradeLock(
			Object object,
			EntityEntry entry,
			LockOptions lockOptions,
			EventSource source) {
		final LockMode requestedLockMode = lockOptions.getLockMode();
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

			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Locking {0} in mode: {1}",
						infoString( entry.getPersister(), entry.getId(), source.getFactory() ),
						requestedLockMode
				);
			}

			final ReactiveActionQueue actionQueue = ( (ReactiveSession) source ).getReactiveActionQueue();
			switch ( requestedLockMode ) {
				case OPTIMISTIC:
					actionQueue.registerProcess( new ReactiveEntityVerifyVersionProcess( object ) );
					entry.setLockMode( requestedLockMode );
					return voidFuture();
				case OPTIMISTIC_FORCE_INCREMENT:
					actionQueue.registerProcess( new ReactiveEntityIncrementVersionProcess( object ) );
					entry.setLockMode( requestedLockMode );
					return voidFuture();
				default:
					return doUpgradeLock( object, entry, lockOptions, source );
			}
		}
		else {
			return voidFuture();
		}
	}

	private CompletionStage<Void> doUpgradeLock(
			Object object, EntityEntry entry,
			LockOptions lockOptions,
			EventSource source) {

		final EntityPersister persister = entry.getPersister();

		final boolean canWriteToCache = persister.canWriteToCache();
		final SoftLock lock;
		final Object cacheKey;
		if ( canWriteToCache ) {
			EntityDataAccess cache = persister.getCacheAccessStrategy();
			cacheKey = cache.generateCacheKey(
					entry.getId(),
					persister,
					source.getFactory(),
					source.getTenantIdentifier()
			);
			lock = cache.lockItem( source, cacheKey, entry.getVersion() );
		}
		else {
			cacheKey = null;
			lock = null;
		}

		try {
			return ( (ReactiveEntityPersister) persister )
					.reactiveLock(
							entry.getId(),
							entry.getVersion(),
							object,
							lockOptions,
							source
					)
					.thenAccept( v -> entry.setLockMode( lockOptions.getLockMode() ) )
					.whenComplete( (r, e) -> {
						// the database now holds a lock + the object is flushed from the cache,
						// so release the soft lock
						if ( canWriteToCache ) {
							persister.getCacheAccessStrategy().unlockItem( source, cacheKey, lock );
						}
					} );
		}
		catch (HibernateException he) {
			//in case lockReactive() throws an exception
			if ( canWriteToCache ) {
				persister.getCacheAccessStrategy().unlockItem( source, cacheKey, lock );
			}
			throw he;
		}
	}
}

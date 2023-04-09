/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

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
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LockEvent;
import org.hibernate.event.spi.LockEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingActions;
import org.hibernate.reactive.engine.impl.ForeignKeys;
import org.hibernate.reactive.engine.impl.ReactiveEntityIncrementVersionProcess;
import org.hibernate.reactive.engine.impl.ReactiveEntityVerifyVersionProcess;
import org.hibernate.reactive.event.ReactiveLockEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.ReactiveSession;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class DefaultReactiveLockEventListener extends AbstractReassociateEventListener
		implements LockEventListener, ReactiveLockEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public CompletionStage<Void> reactiveOnLock(LockEvent event) throws HibernateException {
		if ( event.getObject() == null ) {
			throw new NullPointerException( "attempted to lock null" );
		}

		if ( event.getLockMode() == LockMode.WRITE ) {
			throw LOG.invalidLockModeForLock();
		}

		if ( event.getLockMode() == LockMode.UPGRADE_SKIPLOCKED ) {
			LOG.explicitSkipLockedLockCombo();
		}

		final EventSource source = event.getSession();

		boolean detached = event.getEntityName() != null
				? !source.contains( event.getEntityName(), event.getObject() )
				: !source.contains( event.getObject() );
		if ( detached ) {
			// Hibernate Reactive doesn't support detached instances in refresh()
			throw new IllegalArgumentException("unmanaged instance passed to refresh()");
		}


//		Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );
		//TODO: if object was an uninitialized proxy, this is inefficient,
		//      resulting in two SQL selects

		return ( (ReactiveSession) source ).reactiveFetch( event.getObject(), true )
				.thenCompose( entity -> reactiveOnLock( event, entity ) );
	}

	private CompletionStage<Void> reactiveOnLock(LockEvent event, Object entity) {

		final SessionImplementor source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		final EntityEntry entry = persistenceContext.getEntry(entity);
		final CompletionStage<EntityEntry> stage;
		if ( entry==null ) {
			final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );
			final Object id = persister.getIdentifier( entity, source );
			stage = ForeignKeys.isNotTransient( event.getEntityName(), entity, Boolean.FALSE, source)
					.thenApply( trans -> {
								if (!trans) {
									throw new TransientObjectException(
											"cannot lock an unsaved transient instance: " +
													persister.getEntityName()
									);
								}

								final EntityEntry e = reassociate( event, entity, id, persister );
								cascadeOnLock( event, persister, entity );
								return e;
							}
					);

		}
		else {
			stage = completedFuture( entry );
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

			final ReactiveActionQueue actionQueue = ((ReactiveSession) source).getReactiveActionQueue();
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

	private CompletionStage<Void> doUpgradeLock(Object object, EntityEntry entry,
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
			return ((ReactiveEntityPersister) persister)
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

	@Override
	public void onLock(LockEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}
}

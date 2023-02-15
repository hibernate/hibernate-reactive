/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity;

import static org.hibernate.loader.ast.internal.LoaderHelper.upgradeLock;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.ObjectDeletedException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper.EntityStatus;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper.PersistenceContextEntry;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.sql.results.LoadingLogger;

/**
 * @author Gavin King
 *
 * @see org.hibernate.loader.ast.internal.CacheEntityLoaderHelper
 */
public class ReactiveCacheEntityLoaderHelper {

	public static final ReactiveCacheEntityLoaderHelper INSTANCE = new ReactiveCacheEntityLoaderHelper();

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveCacheEntityLoaderHelper.class );

	private ReactiveCacheEntityLoaderHelper() {}

	/**
	 * Attempts to locate the entity in the session-level cache.
	 * <p>
	 * If allowed to return nulls, then if the entity happens to be found in
	 * the session cache, we check the entity type for proper handling
	 * of entity hierarchies.
	 * <p>
	 * If checkDeleted was set to true, then if the entity is found in the
	 * session-level cache, its current status within the session cache
	 * is checked to see if it has previously been scheduled for deletion.
	 *
	 * @param event The load event
	 * @param keyToLoad The EntityKey representing the entity to be loaded.
	 * @param options The load options.
	 *
	 * @return The entity from the session-level cache, or null.
	 *
	 * @throws HibernateException Generally indicates problems applying a lock-mode.
	 */
	public PersistenceContextEntry loadFromSessionCache(
			final LoadEvent event,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options) throws HibernateException {

		SessionImplementor session = event.getSession();
		Object old = session.getEntityUsingInterceptor( keyToLoad );

		if ( old != null ) {
			// this object was already loaded
			EntityEntry oldEntry = session.getPersistenceContext().getEntry( old );
			if ( options.isCheckDeleted() ) {
				if ( oldEntry.getStatus().isDeletedOrGone() ) {
					LoadingLogger.LOGGER.debug(
							"Load request found matching entity in context, but it is scheduled for removal; returning null" );
					return new PersistenceContextEntry( old, EntityStatus.REMOVED_ENTITY_MARKER );
				}
			}
			if ( options.isAllowNulls() ) {
				final EntityPersister persister = event.getSession()
						.getFactory()
						.getRuntimeMetamodels()
						.getMappingMetamodel()
						.getEntityDescriptor( keyToLoad.getEntityName() );
				if ( !persister.isInstance( old ) ) {
					LOG.debug(
							"Load request found matching entity in context, but the matched entity was of an inconsistent return type; returning null"
					);
					return new PersistenceContextEntry( old, EntityStatus.INCONSISTENT_RTN_CLASS_MARKER );
				}
			}
			upgradeLock( old, oldEntry, event.getLockOptions(), event.getSession() );
		}

		return new PersistenceContextEntry( old, EntityStatus.MANAGED );
	}

	/**
	 * see org.hibernate.event.internal.AbstractLockUpgradeEventListener#upgradeLock(Object, EntityEntry, LockOptions, EventSource)
	 */
	private static void upgradeLock(Object object, EntityEntry entry, LockOptions lockOptions, EventSource source) {

		LockMode requestedLockMode = lockOptions.getLockMode();
		if ( requestedLockMode.greaterThan( entry.getLockMode() ) ) {
			// The user requested a "greater" (i.e. more restrictive) form of pessimistic lock

			if ( entry.getStatus() != Status.MANAGED ) {
				throw new ObjectDeletedException(
						"attempted to lock a deleted instance",
						entry.getId(),
						entry.getPersister().getEntityName()
				);
			}

			final EntityPersister persister = entry.getPersister();
			final boolean cachingEnabled = persister.canWriteToCache();
			SoftLock lock = null;
			Object ck = null;
			try {
				if ( cachingEnabled ) {
					EntityDataAccess cache = persister.getCacheAccessStrategy();
					ck = cache.generateCacheKey( entry.getId(), persister, source.getFactory(), source.getTenantIdentifier() );
					lock = cache.lockItem( source, ck, entry.getVersion() );
				}

				if ( persister.isVersioned() && requestedLockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT  ) {
					// todo : should we check the current isolation mode explicitly?
					Object nextVersion = persister.forceVersionIncrement( entry.getId(), entry.getVersion(), source );
					entry.forceLocked( object, nextVersion );
				}
				else {
					( (ReactiveEntityPersister) persister )
							.reactiveLock( entry.getId(), entry.getVersion(), object, lockOptions, source );
				}
				entry.setLockMode( requestedLockMode );
			}
			finally {
				// the database now holds a lock + the object is flushed from the cache,
				// so release the soft lock
				if ( cachingEnabled ) {
					persister.getCacheAccessStrategy().unlockItem( source, ck, lock );
				}
			}

		}
	}
}

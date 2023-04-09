/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive {@link EntityDeleteAction}.
 */
public class ReactiveEntityDeleteAction extends EntityDeleteAction implements ReactiveExecutable {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveEntityDeleteAction(
			Object id,
			Object[] state,
			Object version,
			Object instance,
			EntityPersister persister,
			boolean isCascadeDeleteEnabled,
			EventSource session) {
		super( id, state, version, instance, persister, isCascadeDeleteEnabled, session );
	}

	public ReactiveEntityDeleteAction(Object id, EntityPersister persister, EventSource session) {
		super( id, persister, session );
	}

	@Override
	public void execute() throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	private boolean isInstanceLoaded() {
		// A null instance signals that we're deleting an unloaded proxy.
		return getInstance() != null;
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {
		final Object id = getId();
		final Object version = getCurrentVersion();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		final boolean veto = isInstanceLoaded() && preDelete();

		final Object ck = lockCacheItem();

		final CompletionStage<Void> deleteStep = !isCascadeDeleteEnabled() && !veto
				? ( (ReactiveEntityPersister) persister ).deleteReactive( id, version, instance, session )
				: voidFuture();

		return deleteStep.thenAccept( v -> {
			if ( isInstanceLoaded() ) {
				postDeleteLoaded( id, persister, session, instance, ck );
			}
			else {
				// we're deleting an unloaded proxy
				postDeleteUnloaded( id, persister, session, ck );
			}

			final StatisticsImplementor statistics = getSession().getFactory().getStatistics();
			if ( statistics.isStatisticsEnabled() && !veto ) {
				statistics.deleteEntity( getPersister().getEntityName() );
			}
		} );
	}

	//TODO: copy/paste from superclass (make it protected!)
	private Object getCurrentVersion() {
		return getPersister().isVersionPropertyGenerated()
						// skip if we're deleting an unloaded proxy, no need for the version
						&& isInstanceLoaded()
				// we need to grab the version value from the entity, otherwise
				// we have issues with generated-version entities that may have
				// multiple actions queued during the same flush
				? getPersister().getVersion( getInstance() )
				: getVersion();
	}

	//TODO: copy/paste of postDeleteLoaded() from superclass (make it protected!)
	private void postDeleteLoaded(
			Object id,
			EntityPersister persister,
			SharedSessionContractImplementor session,
			Object instance,
			Object ck) {
		// After actually deleting a row, record the fact that the instance no longer
		// exists on the database (needed for identity-column key generation), and
		// remove it from the session cache
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final EntityEntry entry = persistenceContext.removeEntry(instance);
		if ( entry == null ) {
			throw new AssertionFailure( "possible non-threadsafe access to session" );
		}
		entry.postDelete();
		final EntityKey key = entry.getEntityKey();
		persistenceContext.removeEntity( key );
		persistenceContext.removeProxy( key );
		removeCacheItem( ck );
		persistenceContext.getNaturalIdResolutions().removeSharedResolution( id, getNaturalIdValues(), persister );
		postDelete();
	}

	//TODO: copy/paste of postDeleteUnloaded() from superclass (make it protected!)
	private void postDeleteUnloaded(Object id, EntityPersister persister, SharedSessionContractImplementor session, Object ck) {
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final EntityKey key = session.generateEntityKey( id, persister );
		if ( !persistenceContext.containsDeletedUnloadedEntityKey( key ) ) {
			throw new AssertionFailure( "deleted proxy should be for an unloaded entity: " + key );
		}
		persistenceContext.removeProxy( key );
		removeCacheItem( ck );
	}

	//TODO: copy/paste from superclass (make it protected!)
	private Object lockCacheItem() {
		final EntityPersister persister = getPersister();
		if ( persister.canWriteToCache() ) {
			final EntityDataAccess cache = persister.getCacheAccessStrategy();
			final SharedSessionContractImplementor session = getSession();
			final Object ck = cache.generateCacheKey( getId(), persister, session.getFactory(), session.getTenantIdentifier() );
			setLock( cache.lockItem( session, ck, getCurrentVersion() ) );
			return ck;
		}
		else {
			return null;
		}
	}

	//TODO: copy/paste from superclass (make it protected!)
	private void removeCacheItem(Object ck) {
		final EntityPersister persister = getPersister();
		if ( persister.canWriteToCache() ) {
			persister.getCacheAccessStrategy().remove( getSession(), ck);
		}
	}
}

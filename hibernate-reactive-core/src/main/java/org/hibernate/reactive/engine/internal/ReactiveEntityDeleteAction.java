/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PreDeleteEvent;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.persister.entity.internal.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

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

	private boolean preDelete() {
		final var listenerGroup = getEventListenerGroups().eventListenerGroup_PRE_DELETE;
		if ( listenerGroup.isEmpty() ) {
			return false;
		}
		else {
			final PreDeleteEvent event =
					new PreDeleteEvent( getInstance(), getId(), getState(), getPersister(), eventSource() );
			boolean veto = false;
			for ( var listener : listenerGroup.listeners() ) {
				veto |= listener.onPreDelete( event );
			}
			return veto;
		}
	}

	private SoftLock lock;

	private Object lockCacheItem() {
		final var persister = getPersister();
		if ( persister.canWriteToCache() ) {
			final var cache = persister.getCacheAccessStrategy();
			final var session = getSession();
			final Object cacheKey = cache.generateCacheKey(
					getId(),
					persister,
					session.getFactory(),
					session.getTenantIdentifier()
			);
			lock = cache.lockItem( session, cacheKey, getCurrentVersion() );
			return cacheKey;
		}
		else {
			return null;
		}
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
		return deleteStep(
				veto,
				(ReactiveEntityPersister) persister,
				id,
				version,
				instance,
				session
		).thenAccept( v -> {
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

	private CompletionStage<Void> deleteStep(
			boolean veto,
			ReactiveEntityPersister persister,
			Object id,
			Object version,
			Object instance,
			SharedSessionContractImplementor session) {
		return !isCascadeDeleteEnabled() && !veto
				? persister.deleteReactive( id, version, instance, session )
				: voidFuture();
	}
}

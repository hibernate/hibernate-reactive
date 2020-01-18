/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.engine.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.*;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.spi.*;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.engine.spi.RxExecutable;
import org.hibernate.rx.persister.entity.impl.RxEntityPersister;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.stat.internal.StatsHelper;
import org.hibernate.stat.spi.StatisticsImplementor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

/**
 * The action for performing an entity insertion, for entities not defined to use IDENTITY generation.
 */
public final class RxEntityInsertAction extends AbstractEntityInsertAction implements RxExecutable {

	private Object version;
	private Object cacheEntry;

	/**
	 * Constructs an EntityInsertAction.
	 *
	 * @param id The entity identifier
	 * @param state The current (extracted) entity state
	 * @param instance The entity instance
	 * @param version The current entity version value
	 * @param descriptor The entity's descriptor
	 * @param isVersionIncrementDisabled Whether version incrementing is disabled.
	 * @param session The session
	 */
	public RxEntityInsertAction(
			Serializable id,
			Object[] state,
			Object instance,
			Object version,
			EntityPersister descriptor,
			boolean isVersionIncrementDisabled,
			SharedSessionContractImplementor session) {
		super( id, state, instance, isVersionIncrementDisabled, descriptor, session );
		this.version = version;
	}

	@Override
	public boolean isEarlyInsert() {
		return false;
	}

	@Override
	protected EntityKey getEntityKey() {
		return getSession().generateEntityKey( getId(), getPersister() );
	}

	@Override
	public void execute() throws HibernateException {
		throw new NotYetImplementedException();
	}

	@Override
	public CompletionStage<Void> rxExecute() throws HibernateException {

		nullifyTransientReferencesIfNotAlready();

		EntityPersister persister = getPersister();
		RxEntityPersister rxPersister = RxEntityPersister.get(persister);
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();
		final Serializable id = getId();

		// FIXME: It needs to become async
		final boolean veto = preInsert();

		// Don't need to lock the cache here, since if someone
		// else inserted the same pk first, the insert would fail
		CompletionStage<Void> insertStage;
		if ( !veto ) {
			insertStage = rxPersister.insertRx( id, getState(), instance, session )
					.thenApply( res -> {
						PersistenceContext persistenceContext = session.getPersistenceContext();
						final EntityEntry entry = persistenceContext.getEntry( instance );
						if ( entry == null ) {
							throw new AssertionFailure( "possible non-threadsafe access to session" );
						}

						entry.postInsert( getState() );

						if ( persister.hasInsertGeneratedProperties() ) {
							persister.processInsertGeneratedProperties( id, instance, getState(), session );
							if ( persister.isVersionPropertyGenerated() ) {
								version = Versioning.getVersion( getState(), persister );
							}
							entry.postUpdate( instance, getState(), version );
						}

						persistenceContext.registerInsertedKey( persister, getId() );
						return null;
					} );
		}
		else {
			insertStage = RxUtil.nullFuture();
		}

		return insertStage.thenApply( res -> {
			final SessionFactoryImplementor factory = session.getFactory();

			if ( isCachePutEnabled( persister, session ) ) {
				final EntityDataAccess cacheAccess = factory.getCache()
						.getEntityRegionAccess( persister.getNavigableRole() );

				final CacheEntry ce = persister.buildCacheEntry(
						instance,
						getState(),
						version,
						session
				);
				cacheEntry = persister.getCacheEntryStructure().structure( ce );
				final EntityDataAccess cache = persister.getCacheAccessStrategy();
				final Object ck = cache.generateCacheKey( id, persister, factory, session.getTenantIdentifier() );

				final boolean put = cacheInsert( persister, ck );

				if ( put && factory.getStatistics().isStatisticsEnabled() ) {
					factory.getStatistics().entityCachePut(
							persister.getNavigableRole(),
							cacheAccess.getRegion().getName()
					);
				}
			}

			handleNaturalIdPostSaveNotifications( id );

			postInsert();

			if ( factory.getStatistics().isStatisticsEnabled() && !veto ) {
				factory.getStatistics().insertEntity( getEntityName() );
			}

			markExecuted();
			return null;
		} );
	}

	private boolean cacheInsert(EntityPersister persister, Object ck) {
		SharedSessionContractImplementor session = this.getSession();

		boolean var4;
		try {
			session.getEventListenerManager().cachePutStart();
			var4 = persister.getCacheAccessStrategy().insert( session, ck, this.cacheEntry, this.version );
		}
		finally {
			session.getEventListenerManager().cachePutEnd();
		}

		return var4;
	}

	private void postInsert() {
		EventListenerGroup<PostInsertEventListener> listenerGroup = this.listenerGroup( EventType.POST_INSERT );
		if ( !listenerGroup.isEmpty() ) {
			PostInsertEvent event = new PostInsertEvent(
					this.getInstance(),
					this.getId(),
					this.getState(),
					this.getPersister(),
					this.eventSource()
			);
			Iterator var3 = listenerGroup.listeners().iterator();

			while ( var3.hasNext() ) {
				PostInsertEventListener listener = (PostInsertEventListener) var3.next();
				listener.onPostInsert( event );
			}

		}
	}

	private void postCommitInsert(boolean success) {
		final EventListenerGroup<PostInsertEventListener> listenerGroup = listenerGroup( EventType.POST_COMMIT_INSERT );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PostInsertEvent event = new PostInsertEvent(
				getInstance(),
				getId(),
				getState(),
				getPersister(),
				eventSource()
		);
		for ( PostInsertEventListener listener : listenerGroup.listeners() ) {
			if ( PostCommitInsertEventListener.class.isInstance( listener ) ) {
				if ( success ) {
					listener.onPostInsert( event );
				}
				else {
					( (PostCommitInsertEventListener) listener ).onPostInsertCommitFailed( event );
				}
			}
			else {
				//default to the legacy implementation that always fires the event
				listener.onPostInsert( event );
			}
		}
	}

	private boolean preInsert() {
		boolean veto = false;

		final EventListenerGroup<PreInsertEventListener> listenerGroup = listenerGroup( EventType.PRE_INSERT );
		if ( listenerGroup.isEmpty() ) {
			return veto;
		}
		final PreInsertEvent event = new PreInsertEvent(
				getInstance(),
				getId(),
				getState(),
				getPersister(),
				eventSource()
		);
		for ( PreInsertEventListener listener : listenerGroup.listeners() ) {
			veto |= listener.onPreInsert( event );
		}
		return veto;
	}

	@Override
	public void doAfterTransactionCompletion(boolean success, SharedSessionContractImplementor session)
			throws HibernateException {
		EntityPersister persister = this.getPersister();
		if ( success && this.isCachePutEnabled( persister, this.getSession() ) ) {
			EntityDataAccess cache = persister.getCacheAccessStrategy();
			SessionFactoryImplementor factory = session.getFactory();
			Object ck = cache.generateCacheKey( this.getId(), persister, factory, session.getTenantIdentifier() );
			boolean put = this.cacheAfterInsert( cache, ck );
			StatisticsImplementor statistics = factory.getStatistics();
			if ( put && statistics.isStatisticsEnabled() ) {
				statistics.entityCachePut(
						StatsHelper.INSTANCE.getRootEntityRole( persister ),
						cache.getRegion().getName()
				);
			}
		}

		this.postCommitInsert( success );
	}

	private boolean cacheAfterInsert(EntityDataAccess cache, Object ck) {
		SharedSessionContractImplementor session = getSession();
		final SessionEventListenerManager eventListenerManager = session.getEventListenerManager();
		try {
			eventListenerManager.cachePutStart();
			return cache.afterInsert( session, ck, cacheEntry, version );
		}
		finally {
			eventListenerManager.cachePutEnd();
		}
	}

	@Override
	protected boolean hasPostCommitEventListeners() {
		final EventListenerGroup<PostInsertEventListener> group = listenerGroup( EventType.POST_COMMIT_INSERT );
		for ( PostInsertEventListener listener : group.listeners() ) {
			if ( listener.requiresPostCommitHandling( getPersister() ) ) {
				return true;
			}
		}

		return false;
	}

	private boolean isCachePutEnabled(EntityPersister persister, SharedSessionContractImplementor session) {
		return persister.canWriteToCache() && !persister.isCacheInvalidationRequired() && session.getCacheMode()
				.isPutEnabled();
	}

}

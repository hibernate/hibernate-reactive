/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.PersistentObjectException;
import org.hibernate.UnresolvableObjectException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.spi.ActionQueue;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.internal.EvictVisitor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.RefreshEvent;
import org.hibernate.event.spi.RefreshEventListener;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingActions;
import org.hibernate.reactive.event.ReactiveRefreshEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveAbstractEntityPersister;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.CollectionType;
import org.hibernate.type.CompositeType;
import org.hibernate.type.Type;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultRefreshEventListener}.
 */
public class DefaultReactiveRefreshEventListener
		implements RefreshEventListener, ReactiveRefreshEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public CompletionStage<Void> reactiveOnRefresh(RefreshEvent event) throws HibernateException {
		return reactiveOnRefresh( event, new IdentitySet( 10 ) );
	}

	@Override
	public void onRefresh(RefreshEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void onRefresh(RefreshEvent event, Map refreshedAlready) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Handle the given refresh event.
	 *
	 * @param event The refresh event to be handled.
	 */
	public CompletionStage<Void> reactiveOnRefresh(RefreshEvent event, IdentitySet refreshedAlready) {

		EventSource source = event.getSession();

		boolean detached = event.getEntityName() != null
				? !source.contains( event.getEntityName(), event.getObject() )
				: !source.contains( event.getObject() );
		if ( detached ) {
			// Hibernate Reactive doesn't support detached instances in refresh()
			throw new IllegalArgumentException("unmanaged instance passed to refresh()");
		}

//		if ( persistenceContext.reassociateIfUninitializedProxy( event.getObject() ) ) {
//			if (detached) {
//				source.setReadOnly( event.getObject(), source.isDefaultReadOnly() );
//			}
//			return CompletionStages.voidFuture();
//		}
//
//		final Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );

		return ( (ReactiveSession) source ).reactiveFetch( event.getObject(), true )
				.thenCompose( entity -> reactiveOnRefresh( event, refreshedAlready, entity ) );
	}

	private CompletionStage<Void> reactiveOnRefresh(RefreshEvent event, IdentitySet refreshedAlready, Object entity) {
		EventSource source = event.getSession();
		PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		if ( refreshedAlready.contains( entity ) ) {
			LOG.trace( "Already refreshed" );
			return voidFuture();
		}

		final EntityEntry e = persistenceContext.getEntry( entity );
		final EntityPersister persister;
		final Serializable id;

		if ( e == null ) {
			persister = source.getEntityPersister(
					event.getEntityName(),
					entity
			); //refresh() does not pass an entityName
			id = persister.getIdentifier( entity, event.getSession() );
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Refreshing transient {0}",
						infoString( persister, id, source.getFactory() )
				);
			}
			if ( persistenceContext.getEntry( source.generateEntityKey( id, persister ) ) != null ) {
				throw new PersistentObjectException(
						"attempted to refresh transient instance when persistent instance was already associated with the session: "
								+ infoString( persister, id, source.getFactory() )
				);
			}
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Refreshing ",
						infoString( e.getPersister(), e.getId(), source.getFactory() )
				);
			}
			if ( !e.isExistsInDatabase() ) {
				throw new UnresolvableObjectException(
						e.getId(),
						"this instance does not yet exist as a row in the database"
				);
			}

			persister = e.getPersister();
			id = e.getId();
		}

		// cascade the refresh prior to refreshing this entity
		refreshedAlready.add( entity );

		return cascadeRefresh(source, persister, entity, refreshedAlready)
				.thenCompose(v -> {

					if ( e != null ) {
						final EntityKey key = source.generateEntityKey( id, persister );
						persistenceContext.removeEntity( key );
						if ( persister.hasCollections() ) {
							new EvictVisitor(source, entity ).process( entity, persister );
						}
					}

					if ( persister.canWriteToCache() ) {
						Object previousVersion = null;
						if ( persister.isVersionPropertyGenerated() ) {
							// we need to grab the version value from the entity, otherwise
							// we have issues with generated-version entities that may have
							// multiple actions queued during the same flush
							previousVersion = persister.getVersion( entity );
						}
						final EntityDataAccess cache = persister.getCacheAccessStrategy();
						final Object ck = cache.generateCacheKey(
								id,
								persister,
								source.getFactory(),
								source.getTenantIdentifier()
						);
						final SoftLock lock = cache.lockItem(source, ck, previousVersion );
						cache.remove(source, ck );
						source.getActionQueue().registerProcess( (success, session) -> cache.unlockItem( session, ck, lock ) );
					}

					evictCachedCollections( persister, id, source);

					String previousFetchProfile = source.getLoadQueryInfluencers().getInternalFetchProfile();
					source.getLoadQueryInfluencers().setInternalFetchProfile( "refresh" );

					// Handle the requested lock-mode (if one) in relation to the entry's (if one) current lock-mode

					LockOptions lockOptionsToUse = event.getLockOptions();

					final LockMode requestedLockMode = lockOptionsToUse.getLockMode();
					final LockMode postRefreshLockMode;

					if ( e != null ) {
						final LockMode currentLockMode = e.getLockMode();
						if ( currentLockMode.greaterThan( requestedLockMode ) ) {
							// the requested lock-mode is less restrictive than the current one
							//		- pass along the current lock-mode (after accounting for WRITE)
							lockOptionsToUse = LockOptions.copy( event.getLockOptions(), new LockOptions() );
							if ( currentLockMode == LockMode.WRITE ||
									currentLockMode == LockMode.PESSIMISTIC_WRITE ||
									currentLockMode == LockMode.PESSIMISTIC_READ ) {
								// our transaction should already hold the exclusive lock on
								// the underlying row - so READ should be sufficient.
								//
								// in fact, this really holds true for any current lock-mode that indicates we
								// hold an exclusive lock on the underlying row - but we *need* to handle
								// WRITE specially because the Loader/Locker mechanism does not allow for WRITE
								// locks
								lockOptionsToUse.setLockMode( LockMode.READ );

								// and prepare to reset the entry lock-mode to the previous lock mode after
								// the refresh completes
								postRefreshLockMode = currentLockMode;
							}
							else {
								lockOptionsToUse.setLockMode( currentLockMode );
								postRefreshLockMode = null;
							}
						}
						else {
							postRefreshLockMode = null;
						}
					}
					else {
						postRefreshLockMode = null;
					}

					return ( (ReactiveAbstractEntityPersister) persister ).reactiveLoad( id, entity, lockOptionsToUse, source)
							.thenAccept(result -> {
								if ( result!=null ) {

									// apply `postRefreshLockMode`, if needed
									if (postRefreshLockMode != null) {
										// if we get here, there was a previous entry and we need to re-set its lock-mode
										//		- however, the refresh operation actually creates a new entry, so get it
										persistenceContext.getEntry(result).setLockMode(postRefreshLockMode);
									}

									// Keep the same read-only/modifiable setting for the entity that it had before refreshing;
									// If it was transient, then set it to the default for the source.
									if (!persister.isMutable()) {
										// this is probably redundant; it should already be read-only
										source.setReadOnly(result, true);
									}
									else {
										source.setReadOnly(result, e == null ? source.isDefaultReadOnly() : e.isReadOnly());
									}
								}

								UnresolvableObjectException.throwIfNull(result, id, persister.getEntityName());
							})
							.whenComplete( (vv,t) -> source.getLoadQueryInfluencers().setInternalFetchProfile(previousFetchProfile) );
				} );
	}

	private CompletionStage<Void> cascadeRefresh(
			EventSource source,
			EntityPersister persister,
			Object object,
			IdentitySet refreshedAlready) {
		return new Cascade<>(
				CascadingActions.REFRESH,
				CascadePoint.BEFORE_REFRESH,
				persister,
				object,
				refreshedAlready,
				source
		).cascade();
	}

	private void evictCachedCollections(EntityPersister persister, Serializable id, EventSource source) {
		evictCachedCollections( persister.getPropertyTypes(), id, source );
	}

	private void evictCachedCollections(Type[] types, Serializable id, EventSource source)
			throws HibernateException {
		final ActionQueue actionQueue = source.getActionQueue();
		final SessionFactoryImplementor factory = source.getFactory();
		final MetamodelImplementor metamodel = factory.getMetamodel();
		for ( Type type : types ) {
			if ( type.isCollectionType() ) {
				CollectionPersister collectionPersister = metamodel.collectionPersister( ( (CollectionType) type ).getRole() );
				if ( collectionPersister.hasCache() ) {
					final CollectionDataAccess cache = collectionPersister.getCacheAccessStrategy();
					final Object ck = cache.generateCacheKey(
						id,
						collectionPersister,
						factory,
						source.getTenantIdentifier()
					);
					final SoftLock lock = cache.lockItem( source, ck, null );
					cache.remove( source, ck );
					actionQueue.registerProcess( (success, session) -> cache.unlockItem( session, ck, lock ) );
				}
			}
			else if ( type.isComponentType() ) {
				CompositeType actype = (CompositeType) type;
				evictCachedCollections( actype.getSubtypes(), id, source );
			}
		}
	}

}

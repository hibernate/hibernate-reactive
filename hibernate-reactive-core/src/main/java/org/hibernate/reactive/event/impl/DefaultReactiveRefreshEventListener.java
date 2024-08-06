/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
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
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.internal.EvictVisitor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.RefreshContext;
import org.hibernate.event.spi.RefreshEvent;
import org.hibernate.event.spi.RefreshEventListener;
import org.hibernate.loader.ast.spi.CascadingFetchProfile;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
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
		return reactiveOnRefresh( event, RefreshContext.create() );
	}

	@Override
	public void onRefresh(RefreshEvent event) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveOnRefresh" );
	}

	@Override
	public void onRefresh(RefreshEvent event, RefreshContext refreshedAlready) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveOnRefresh" );
	}

	/**
	 * Handle the given refresh event.
	 *
	 * @param event The refresh event to be handled.
	 */
	@Override
	public CompletionStage<Void> reactiveOnRefresh(RefreshEvent event, RefreshContext refreshedAlready) {
		final EventSource source = event.getSession();

		boolean detached = event.getEntityName() != null
				? !source.contains( event.getEntityName(), event.getObject() )
				: !source.contains( event.getObject() );

		if ( detached ) {
			// Hibernate Reactive doesn't support detached instances in refresh()
			throw new IllegalArgumentException( "Unmanaged instance passed to refresh()" );
		}
		return ( (ReactiveSession) source )
				.reactiveFetch( event.getObject(), true )
				.thenCompose( entity -> reactiveOnRefresh( event, refreshedAlready, entity ) );
	}

	private CompletionStage<Void> reactiveOnRefresh(RefreshEvent event, RefreshContext refreshedAlready, Object object) {
		final EventSource source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		if ( persistenceContext.reassociateIfUninitializedProxy( object ) ) {
			if ( isTransient( event, source, object ) ) {
				source.setReadOnly( object,  source.isDefaultReadOnly() );
			}
			return voidFuture();
		}

		Object entity = persistenceContext.unproxyAndReassociate( object );
		if ( !refreshedAlready.add( entity) ) {
			LOG.trace( "Already refreshed" );
			return voidFuture();
		}

		final EntityEntry entry = persistenceContext.getEntry( entity );

		final EntityPersister persister;
		final Object id;
		if ( entry == null ) {
			//refresh() does not pass an entityName
			persister = source.getEntityPersister( event.getEntityName(), entity );
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
						infoString( entry.getPersister(), entry.getId(), source.getFactory() )
				);
			}
			if ( !entry.isExistsInDatabase() ) {
				throw new UnresolvableObjectException(
						entry.getId(),
						"this instance does not yet exist as a row in the database"
				);
			}

			persister = entry.getPersister();
			id = entry.getId();
		}

		// cascade the refresh prior to refreshing this entity
		return cascadeRefresh( source, persister, entity, refreshedAlready )
				.thenCompose( v -> {
					if ( entry != null ) {
						persistenceContext.removeEntityHolder( entry.getEntityKey() );
						if ( persister.hasCollections() ) {
							new EvictVisitor( source, object ).process( object, persister );
						}
						persistenceContext.removeEntry( object );
					}

					evictEntity( entity, persister, id, source );
					evictCachedCollections( persister, id, source );

					final CompletableFuture<Void> refresh = new CompletableFuture<>();
					source.getLoadQueryInfluencers()
							.fromInternalFetchProfile(
									CascadingFetchProfile.REFRESH,
									() -> doRefresh( event, source, entity, entry, persister, id, persistenceContext )
											.whenComplete( (unused, throwable) -> {
												if ( throwable == null ) {
													refresh.complete( null );
												}
												else {
													refresh.completeExceptionally( throwable );
												}
											} )
							);
					return refresh;
				} );
	}

	private static boolean isTransient(RefreshEvent event, EventSource source, Object object) {
		final String entityName = event.getEntityName();
		return entityName != null ? !source.contains( entityName, object) : !source.contains(object);
	}

	private static void evictEntity(Object entity, EntityPersister persister, Object id, EventSource source) {
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
			final SoftLock lock = cache.lockItem( source, ck, previousVersion );
			cache.remove(source, ck );
			source.getActionQueue().registerProcess( (success, session) -> cache.unlockItem( session, ck, lock ) );
		}
	}

	private static CompletionStage<Void> doRefresh(
			RefreshEvent event,
			EventSource source,
			Object entity,
			EntityEntry entry,
			EntityPersister persister,
			Object id,
			PersistenceContext persistenceContext) {
		// Handle the requested lock-mode (if one) in relation to the entry's (if one) current lock-mode
		LockOptions lockOptionsToUse = event.getLockOptions();
		final LockMode requestedLockMode = lockOptionsToUse.getLockMode();
		final LockMode postRefreshLockMode;
		if ( entry != null ) {
			final LockMode currentLockMode = entry.getLockMode();
			if ( currentLockMode.greaterThan( requestedLockMode ) ) {
				// the requested lock-mode is less restrictive than the current one
				//		- pass along the current lock-mode (after accounting for WRITE)
				lockOptionsToUse = event.getLockOptions().makeCopy();
				if ( currentLockMode == LockMode.WRITE
						|| currentLockMode == LockMode.PESSIMISTIC_WRITE
						|| currentLockMode == LockMode.PESSIMISTIC_READ ) {
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

		return ( (ReactiveAbstractEntityPersister) persister )
				.reactiveLoad( id, entity, lockOptionsToUse, source )
				.thenAccept( result -> {
					if ( result != null ) {
						// apply `postRefreshLockMode`, if needed
						if ( postRefreshLockMode != null ) {
							// if we get here, there was a previous entry, and we need to re-set its lock-mode
							//		- however, the refresh operation actually creates a new entry, so get it
							persistenceContext.getEntry( result ).setLockMode( postRefreshLockMode );
						}

						// Keep the same read-only/modifiable setting for the entity that it had before refreshing;
						// If it was transient, then set it to the default for the source.
						if ( !persister.isMutable() ) {
							// this is probably redundant; it should already be read-only
							source.setReadOnly( result, true );
						}
						else {
							source.setReadOnly( result, entry == null ? source.isDefaultReadOnly() : entry.isReadOnly() );
						}
					}

					UnresolvableObjectException.throwIfNull( result, id, persister.getEntityName() );
				} );
	}

	private CompletionStage<Void> cascadeRefresh(
			EventSource source,
			EntityPersister persister,
			Object object,
			RefreshContext refreshedAlready) {
		return Cascade.cascade(
				CascadingActions.REFRESH,
				CascadePoint.BEFORE_REFRESH,
				source,
				persister,
				object,
				refreshedAlready
		);
	}

	private void evictCachedCollections(EntityPersister persister, Object id, EventSource source) {
		evictCachedCollections( persister.getPropertyTypes(), id, source );
	}

	private void evictCachedCollections(Type[] types, Object id, EventSource source)
			throws HibernateException {
		final ActionQueue actionQueue = source.getActionQueue();
		final SessionFactoryImplementor factory = source.getFactory();
		final MappingMetamodelImplementor metamodel = factory.getRuntimeMetamodels().getMappingMetamodel();
		for ( Type type : types ) {
			if ( type.isCollectionType() ) {
				final String role = ((CollectionType) type).getRole();
				final CollectionPersister collectionPersister = metamodel.getCollectionDescriptor( role );
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
				final CompositeType compositeType = (CompositeType) type;
				evictCachedCollections( compositeType.getSubtypes(), id, source );
			}
		}
	}

}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import org.hibernate.*;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.spi.*;
import org.hibernate.event.internal.EntityState;
import org.hibernate.event.internal.EventUtil;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.event.spi.*;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingAction;
import org.hibernate.reactive.event.spi.ReactiveMergeEventListener;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.TypeHelper;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultMergeEventListener}.
 */
public class DefaultReactiveMergeEventListener extends AbstractReactiveSaveEventListener<MergeContext> implements ReactiveMergeEventListener, MergeEventListener {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultReactiveMergeEventListener.class );

	@Override
	public void onMerge(MergeEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void onMerge(MergeEvent event, Map copiedAlready) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Map<?,?> getMergeMap(Object anything) {
		return ( (MergeContext) anything ).invertMap();
	}

	/**
	 * Handle the given merge event.
	 *
	 * @param event The merge event to be handled.
	 */
	@Override
	public CompletionStage<Void> reactiveOnMerge(MergeEvent event) throws HibernateException {
		final EntityCopyObserver entityCopyObserver = createEntityCopyObserver( event.getSession().getFactory() );
		final MergeContext mergeContext = new MergeContext( event.getSession(), entityCopyObserver );
		return reactiveOnMerge(event, mergeContext)
				.thenAccept(v -> entityCopyObserver.topLevelMergeComplete(event.getSession()))
				.whenComplete((v, e) -> {
					entityCopyObserver.clear();
					mergeContext.clear();
				});
	}

	private EntityCopyObserver createEntityCopyObserver(SessionFactoryImplementor sessionFactory) {
		final ServiceRegistry serviceRegistry = sessionFactory.getServiceRegistry();
		final EntityCopyObserverFactory configurationService = serviceRegistry.getService( EntityCopyObserverFactory.class );
		return configurationService.createEntityCopyObserver();
	}

	/**
	 * Handle the given merge event.
	 *
	 * @param event The merge event to be handled.
	 */
	@Override
	public CompletionStage<Void> reactiveOnMerge(MergeEvent event, MergeContext copyCache) throws HibernateException {

		final EventSource source = event.getSession();
		final Object original = event.getOriginal();

		// NOTE : `original` is the value being merged

		if ( original != null ) {
			final Object entity;
			if ( original instanceof HibernateProxy ) {
				LazyInitializer li = ( (HibernateProxy) original ).getHibernateLazyInitializer();
				if ( li.isUninitialized() ) {
					LOG.trace( "Ignoring uninitialized proxy" );
					event.setResult( source.load( li.getEntityName(), li.getIdentifier() ) );
					//EARLY EXIT!
					return CompletionStages.nullFuture();
				}
				else {
					entity = li.getImplementation();
				}
			}
			else if ( original instanceof PersistentAttributeInterceptable ) {
				final PersistentAttributeInterceptable interceptable = (PersistentAttributeInterceptable) original;
				final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
				if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor ) {
					final EnhancementAsProxyLazinessInterceptor proxyInterceptor = (EnhancementAsProxyLazinessInterceptor) interceptor;
					LOG.trace( "Ignoring uninitialized enhanced-proxy" );
					//no need to go async, AFAICT ?
					event.setResult(source.load(proxyInterceptor.getEntityName(), (Serializable) proxyInterceptor.getIdentifier()));
					//EARLY EXIT!
					return CompletionStages.nullFuture();
				}
				else {
					entity = original;
				}
			}
			else {
				entity = original;
			}

			if ( copyCache.containsKey( entity ) && ( copyCache.isOperatedOn( entity ) ) ) {
				LOG.trace( "Already in merge process" );
				event.setResult( entity );
			}
			else {
				if ( copyCache.containsKey( entity ) ) {
					LOG.trace( "Already in copyCache; setting in merge process" );
					copyCache.setOperatedOn( entity, true );
				}
				event.setEntity( entity );
				EntityState entityState = null;

				// Check the persistence context for an entry relating to this
				// entity to be merged...
				final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
				EntityEntry entry = persistenceContext.getEntry( entity );
				if ( entry == null ) {
					EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );
					Serializable id = persister.getIdentifier( entity, source );
					if ( id != null ) {
						final EntityKey key = source.generateEntityKey( id, persister );
						final Object managedEntity = persistenceContext.getEntity( key );
						entry = persistenceContext.getEntry( managedEntity );
						if ( entry != null ) {
							// we have specialized case of a detached entity from the
							// perspective of the merge operation.  Specifically, we
							// have an incoming entity instance which has a corresponding
							// entry in the current persistence context, but registered
							// under a different entity instance
							entityState = EntityState.DETACHED;
						}
					}
				}

				if ( entityState == null ) {
					entityState = EntityState.getEntityState( entity, event.getEntityName(), entry, source, false );
				}

				switch ( entityState ) {
					case DETACHED:
						return entityIsDetached( event, copyCache );
					case TRANSIENT:
						return entityIsTransient( event, copyCache );
					case PERSISTENT:
						return entityIsPersistent( event, copyCache );
					default: //DELETED
						throw new ObjectDeletedException(
								"deleted instance passed to merge",
								null,
								EventUtil.getLoggableName( event.getEntityName(), entity )
						);
				}
			}

		}

		return CompletionStages.nullFuture();
	}

	protected CompletionStage<Void> entityIsPersistent(MergeEvent event, MergeContext copyCache) {
		LOG.trace( "Ignoring persistent instance" );

		//TODO: check that entry.getIdentifier().equals(requestedId)

		final Object entity = event.getEntity();
		final EventSource source = event.getSession();
		final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );

		copyCache.put( entity, entity, true );  //before cascade!

		return cascadeOnMerge( source, persister, entity, copyCache )
				.thenCompose( v -> fetchAndCopyValues( persister, entity, entity, source, copyCache ) )
				.thenAccept( v -> event.setResult( entity ) );
	}

	protected CompletionStage<Void> entityIsTransient(MergeEvent event, MergeContext copyCache) {

		LOG.trace( "Merging transient instance" );

		final Object entity = event.getEntity();
		final EventSource session = event.getSession();

		final String entityName = event.getEntityName();
		final EntityPersister persister = session.getEntityPersister( entityName, entity );

		final Serializable id = persister.hasIdentifierProperty()
				? persister.getIdentifier( entity, session )
				: null;

		final Object copy;
		final Object existingCopy = copyCache.get( entity );
		if ( existingCopy != null ) {
			persister.setIdentifier( copyCache.get( entity ), id, session );
			copy = existingCopy;
		}
		else {
			copy = session.instantiate( persister, id );

			//before cascade!
			copyCache.put( entity, copy, true );
		}

		// cascade first, so that all unsaved objects get their
		// copy created before we actually copy
		//cascadeOnMerge(event, persister, entity, copyCache, Cascades.CASCADE_BEFORE_MERGE);
		return super.cascadeBeforeSave( session, persister, entity, copyCache )
				.thenAccept( v -> copyValues( persister, entity, copy, session, copyCache, ForeignKeyDirection.FROM_PARENT ) )
				.thenCompose( v -> saveTransientEntity( copy, entityName, event.getRequestedId(), session, copyCache ) )
				.thenCompose( v -> super.cascadeAfterSave( session, persister, entity, copyCache ) )
				.thenAccept( v -> {
					copyValues(persister, entity, copy, session, copyCache, ForeignKeyDirection.TO_PARENT);

					event.setResult(copy);

					if (copy instanceof PersistentAttributeInterceptable) {
						final PersistentAttributeInterceptable interceptable = (PersistentAttributeInterceptable) copy;
						final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
						if (interceptor == null) {
							persister.getBytecodeEnhancementMetadata().injectInterceptor(copy, id, session);
						}
					}
				});
	}

	private CompletionStage<Void> saveTransientEntity(
			Object entity,
			String entityName,
			Serializable requestedId,
			EventSource source,
			MergeContext copyCache) {
		//this bit is only *really* absolutely necessary for handling
		//requestedId, but is also good if we merge multiple object
		//graphs, since it helps ensure uniqueness
		if ( requestedId == null ) {
			return reactiveSaveWithGeneratedId( entity, entityName, copyCache, source, false );
		}
		else {
			return reactiveSaveWithRequestedId( entity, requestedId, entityName, copyCache, source );
		}
	}

	protected CompletionStage<Void> entityIsDetached(MergeEvent event, MergeContext copyCache) {

		LOG.trace( "Merging detached instance" );

		final Object entity = event.getEntity();
		final EventSource source = event.getSession();

		final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );
		final String entityName = persister.getEntityName();

		Serializable requestedId = event.getRequestedId();
		Serializable id;
		if ( requestedId == null ) {
			id = persister.getIdentifier( entity, source );
		}
		else {
			id = requestedId;
			// check that entity id = requestedId
			Serializable entityId = persister.getIdentifier( entity, source );
			if ( !persister.getIdentifierType().isEqual( id, entityId, source.getFactory() ) ) {
				throw new HibernateException( "merge requested with id not matching id of passed entity" );
			}
		}

		String previousFetchProfile = source.getLoadQueryInfluencers().getInternalFetchProfile();
		source.getLoadQueryInfluencers().setInternalFetchProfile( "merge" );

		//we must clone embedded composite identifiers, or
		//we will get back the same instance that we pass in
		final Serializable clonedIdentifier = (Serializable)
				persister.getIdentifierType().deepCopy( id, source.getFactory() );

		return source.unwrap(ReactiveSession.class)
				.reactiveGet( (Class<?>) persister.getMappedClass(), clonedIdentifier )
				.thenCompose(result -> {
					if ( result!=null ) {
						// before cascade!
						copyCache.put(entity, result, true);

						Object target = unproxyManagedForDetachedMerging(entity, result, persister, source);
						if (target == entity) {
							throw new AssertionFailure("entity was not detached");
						}
						else if ( !source.getEntityName(target).equals(entityName) ) {
							throw new WrongClassException(
									"class of the given object did not match class of persistent copy",
									event.getRequestedId(),
									entityName
							);
						}
						else if ( isVersionChanged(entity, source, persister, target) ) {
							final StatisticsImplementor statistics = source.getFactory().getStatistics();
							if (statistics.isStatisticsEnabled()) {
								statistics.optimisticFailure(entityName);
							}
							throw new StaleObjectStateException(entityName, id);
						}

						// cascade first, so that all unsaved objects get their
						// copy created before we actually copy
						return cascadeOnMerge( source, persister, entity, copyCache )
								.thenCompose( v -> fetchAndCopyValues( persister, entity, target, source, copyCache ) )
								.thenAccept(v -> {
									// copyValues() (called by fetchAndCopyValues) works by reflection,
									// so explicitly mark the entity instance dirty
									markInterceptorDirty(entity, target, persister);
									event.setResult(result);
								});
					}
					else {
						//TODO: we should throw an exception if we really *know* for sure
						//      that this is a detached instance, rather than just assuming
						//throw new StaleObjectStateException(entityName, id);

						// we got here because we assumed that an instance
						// with an assigned id was detached, when it was
						// really persistent
						return entityIsTransient(event, copyCache);
					}
				})
				.whenComplete( (v,e) -> source.getLoadQueryInfluencers().setInternalFetchProfile(previousFetchProfile) );

	}

	private Object unproxyManagedForDetachedMerging(
			Object incoming,
			Object managed,
			EntityPersister persister,
			EventSource source) {
		if ( managed instanceof HibernateProxy ) {
			return source.getPersistenceContextInternal().unproxy( managed );
		}

		if ( incoming instanceof PersistentAttributeInterceptable
				&& persister.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading()
				&& source.getSessionFactory().getSessionFactoryOptions().isEnhancementAsProxyEnabled() ) {

			final PersistentAttributeInterceptor incomingInterceptor = ( (PersistentAttributeInterceptable) incoming ).$$_hibernate_getInterceptor();
			final PersistentAttributeInterceptor managedInterceptor = ( (PersistentAttributeInterceptable) managed ).$$_hibernate_getInterceptor();

			// todo - do we need to specially handle the case where both `incoming` and `managed` are initialized, but
			//		with different attributes initialized?
			// 		- for now, assume we do not...

			// if the managed entity is not a proxy, we can just return it
			if ( ! ( managedInterceptor instanceof EnhancementAsProxyLazinessInterceptor ) ) {
				return managed;
			}

			// if the incoming entity is still a proxy there is no need to force initialization of the managed one
			if ( incomingInterceptor instanceof EnhancementAsProxyLazinessInterceptor ) {
				return managed;
			}

			// otherwise, force initialization
			//TODO: probably needs to be made async!
			return persister.initializeEnhancedEntityUsedAsProxy( managed, null, source );
		}

		return managed;
	}

	private void markInterceptorDirty(final Object entity, final Object target, EntityPersister persister) {
		// for enhanced entities, copy over the dirty attributes
		if ( entity instanceof SelfDirtinessTracker && target instanceof SelfDirtinessTracker ) {
			// clear, because setting the embedded attributes dirties them
			( (SelfDirtinessTracker) target ).$$_hibernate_clearDirtyAttributes();

			for ( String fieldName : ( (SelfDirtinessTracker) entity ).$$_hibernate_getDirtyAttributes() ) {
				( (SelfDirtinessTracker) target ).$$_hibernate_trackChange( fieldName );
			}
		}
	}

	private boolean isVersionChanged(Object entity, EventSource source, EntityPersister persister, Object target) {
		if ( !persister.isVersioned() ) {
			return false;
		}
		// for merging of versioned entities, we consider the version having
		// been changed only when:
		// 1) the two version values are different;
		//      *AND*
		// 2) The target actually represents database state!
		//
		// This second condition is a special case which allows
		// an entity to be merged during the same transaction
		// (though during a seperate operation) in which it was
		// originally persisted/saved
		boolean changed = !persister.getVersionType().isSame(
				persister.getVersion( target ),
				persister.getVersion( entity )
		);

		// TODO : perhaps we should additionally require that the incoming entity
		// version be equivalent to the defined unsaved-value?
		return changed && existsInDatabase( target, source, persister );
	}

	private boolean existsInDatabase(Object entity, EventSource source, EntityPersister persister) {
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		EntityEntry entry = persistenceContext.getEntry( entity );
		if ( entry == null ) {
			Serializable id = persister.getIdentifier( entity, source );
			if ( id != null ) {
				final EntityKey key = source.generateEntityKey( id, persister );
				final Object managedEntity = persistenceContext.getEntity( key );
				entry = persistenceContext.getEntry( managedEntity );
			}
		}

		return entry != null && entry.isExistsInDatabase();
	}

	private CompletionStage<Void> fetchAndCopyValues(
			final EntityPersister persister,
			final Object entity,
			final Object target,
			final SessionImplementor source,
			final MergeContext mergeContext) {
		CompletionStage<Void> stage = CompletionStages.nullFuture();
		// If entity == target, then nothing needs to be fetched.
		if ( entity != target ) {
			ReactiveSession session = source.unwrap(ReactiveSession.class);
			final Object[] mergeState = persister.getPropertyValues(entity);
			final Object[] managedState = persister.getPropertyValues(target);
			for (int i = 0; i < mergeState.length; i++) {
				Object fetchable = managedState[i];
				// Cascade-merge mappings do not determine what needs to be fetched.
				// The value only needs to be fetched if the incoming value (mergeState[i])
				// is initialized, but its corresponding managed state is not initialized.
				// Initialization must be done before copyValues executes.
				if ( Hibernate.isInitialized( mergeState[i] ) && !Hibernate.isInitialized( managedState[i] ) ) {
					stage = stage.thenCompose( v -> session.reactiveFetch( fetchable, true ) ).thenApply(ignore -> null);
				}
			}
		}
		return stage.thenAccept( v -> copyValues( persister, entity, target, source, mergeContext ) );
	}

	protected void copyValues(
			final EntityPersister persister,
			final Object entity,
			final Object target,
			final SessionImplementor source,
			final MergeContext copyCache) {
		final Object[] copiedValues = TypeHelper.replace(
				persister.getPropertyValues( entity ),
				persister.getPropertyValues( target ),
				persister.getPropertyTypes(),
				source,
				target,
				copyCache
		);

		persister.setPropertyValues( target, copiedValues );
	}

	protected void copyValues(
			final EntityPersister persister,
			final Object entity,
			final Object target,
			final SessionImplementor source,
			final MergeContext copyCache,
			final ForeignKeyDirection foreignKeyDirection) {

		final Object[] copiedValues;

		if ( foreignKeyDirection == ForeignKeyDirection.TO_PARENT ) {
			// this is the second pass through on a merge op, so here we limit the
			// replacement to associations types (value types were already replaced
			// during the first pass)
			copiedValues = TypeHelper.replaceAssociations(
					persister.getPropertyValues( entity ),
					persister.getPropertyValues( target ),
					persister.getPropertyTypes(),
					source,
					target,
					copyCache,
					foreignKeyDirection
			);
		}
		else {
			copiedValues = TypeHelper.replace(
					persister.getPropertyValues( entity ),
					persister.getPropertyValues( target ),
					persister.getPropertyTypes(),
					source,
					target,
					copyCache,
					foreignKeyDirection
			);
		}

		persister.setPropertyValues( target, copiedValues );
	}

	/**
	 * Perform any cascades needed as part of this copy event.
	 *  @param source The merge event being processed.
	 * @param persister The persister of the entity being copied.
	 * @param entity The entity being copied.
	 * @param copyCache A cache of already copied instance.
	 */
	protected CompletionStage<Void> cascadeOnMerge(
			final EventSource source,
			final EntityPersister persister,
			final Object entity,
			final MergeContext copyCache
	) {
		return new Cascade<>(
				getCascadeReactiveAction(),
				CascadePoint.BEFORE_MERGE,
				persister,
				entity,
				copyCache,
				source
		).cascade();
	}


	@Override
	protected CascadingAction<MergeContext> getCascadeReactiveAction() {
		return org.hibernate.reactive.engine.impl.CascadingActions.MERGE;
	}

	/**
	 * Cascade behavior is redefined by this subclass, disable superclass behavior
	 */
	@Override
	protected CompletionStage<Void> cascadeAfterSave(EventSource source, EntityPersister persister, Object entity, MergeContext anything)
			throws HibernateException {
		return CompletionStages.nullFuture();
	}

	/**
	 * Cascade behavior is redefined by this subclass, disable superclass behavior
	 */
	@Override
	protected CompletionStage<Void> cascadeBeforeSave(EventSource source, EntityPersister persister, Object entity, MergeContext anything)
			throws HibernateException {
		return CompletionStages.nullFuture();
	}

}

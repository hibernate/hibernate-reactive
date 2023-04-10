/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.PersistentObjectException;
import org.hibernate.TypeMismatchException;
import org.hibernate.action.internal.DelayedPostInsertIdentifier;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper.PersistenceContextEntry;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.AttributeMappingsList;
import org.hibernate.metamodel.mapping.CompositeIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.MappingType;
import org.hibernate.metamodel.mapping.NonAggregatedIdentifierMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.event.ReactiveLoadEventListener;
import org.hibernate.reactive.loader.entity.ReactiveCacheEntityLoaderHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.session.impl.SessionUtil.throwEntityNotFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.returnNullorRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive {@link org.hibernate.event.internal.DefaultLoadEventListener}.
 * <p>
 *     Note that sometimes Hibernate ORM calls {@link org.hibernate.internal.SessionImpl#internalLoad(String, Object, boolean, boolean)}
 *     and {@link #onLoad(LoadEvent, LoadType)} is called. We only support this case when loading generates a proxy.
 * </p>
 * <p>
 *     The return value of the private methods loading the entity is a proxy or a {@link CompletionStage}.
 *     The {@link CompletionStage} only happens when we query the db and a proxy is not created.
 *     If {@link #onLoad(LoadEvent, LoadType)} is called, we cannot get the entity loaded from the db without blocking
 *     the request and therefore we aren't going to support this case for now.
 * </p>
 */
public class DefaultReactiveLoadEventListener implements LoadEventListener, ReactiveLoadEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * This method is not reactive, but we expect it to be called only when a proxy can be returned.
	 * <p>
	 *     In particular, it should be called only by
	 *    {@link org.hibernate.internal.SessionImpl#internalLoad(String, Object, boolean, boolean)}.
	 * </p>
	 *
	 * @see org.hibernate.event.internal.DefaultLoadEventListener#onLoad(LoadEvent, LoadType)
	 * @throws UnsupportedOperationException if the entity loaded is not a proxy
	 * @throws UnexpectedAccessToTheDatabase if it needs to load the entity from the db
	 */
	@Override
	public void onLoad(LoadEvent event, LoadType loadType) throws HibernateException {
		final EntityPersister persister = getPersister( event );

		if ( persister == null ) {
			throw LOG.unableToLocatePersister( event.getEntityClassName() );
		}

		// Since this method is not reactive, we're not expecting to hit the
		// database here (if we do, it's a bug) and so we can assume the
		// returned CompletionStage is already completed
		final CompletionStage<Void> checkId = checkId( event, loadType, persister );
		if ( !checkId.toCompletableFuture().isDone() ) {
			// This only happens if the object is loaded from the db
			throw new UnexpectedAccessToTheDatabase();
		}

		try {
			// Since this method is not reactive, we're not expecting to hit the
			// database here (if we do, it's a bug) and so we can assume the
			// returned CompletionStage is already completed (a proxy, perhaps)
			final CompletionStage<Object> loaded = doOnLoad( persister, event, loadType );
			if ( !loaded.toCompletableFuture().isDone() ) {
				// This only happens if the object is loaded from the db
				throw new UnexpectedAccessToTheDatabase();
			}
			else {
				// Proxy
				event.setResult( loaded.toCompletableFuture().getNow( null ) );
			}
		}
		catch (HibernateException e) {
			LOG.unableToLoadCommand( e );
			throw e;
		}

		if ( event.getResult() instanceof CompletionStage ) {
			throw new AssertionFailure( "Unexpected CompletionStage" );
		}
	}

	/**
	 * Handle the given load event.
	 *
	 * @param event The load event to be handled.
	 */
	@Override
	public CompletionStage<Void> reactiveOnLoad(LoadEvent event, LoadType loadType) throws HibernateException {
		final ReactiveEntityPersister persister = (ReactiveEntityPersister) getPersister( event );
		if ( persister == null ) {
			throw LOG.unableToLocatePersister( event.getEntityClassName() );
		}

		return checkId( event, loadType, persister )
				.thenCompose( vd -> doOnLoad( persister, event, loadType )
						.thenAccept( event::setResult )
						.handle( (v, x) -> {
							if ( event.getResult() instanceof CompletionStage ) {
								throw new AssertionFailure( "Unexpected CompletionStage" );
							}
							if ( x instanceof HibernateException ) {
								LOG.unableToLoadCommand( (HibernateException) x );
							}
							return returnNullorRethrow( x );
						} ) )
				.thenCompose( v -> {
					// if a pessimistic version increment was requested, we need
					// to go back to the database immediately and update the row
					// we handle this here instead of in DefaultReactivePostLoadEventListener
					if ( event.getLockMode() == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
						// TODO: should we call CachedDomainDataAccess.lockItem() ?
						return persister.reactiveLock(
								event.getEntityId(),
								persister.getVersion( event.getResult() ),
								event.getResult(),
								event.getLockOptions(),
								event.getSession()
						);
					}
					else {
						return voidFuture();
					}
				} );
	}

	private CompletionStage<Void> checkId(LoadEvent event, LoadType loadType, EntityPersister persister) {
		final Class<?> idClass = persister.getIdentifierType().getReturnedClass();
		if ( idClass != null
				&& !idClass.isInstance( event.getEntityId() )
				&& !( event.getEntityId() instanceof DelayedPostInsertIdentifier ) ) {
			return checkIdClass( persister, event, loadType, idClass );
		}
		return voidFuture();
	}

	protected EntityPersister getPersister(final LoadEvent event) {
		final Object instanceToLoad = event.getInstanceToLoad();
		final EventSource source = event.getSession();
		if ( instanceToLoad != null ) {
			//the load() which takes an entity does not pass an entityName
			event.setEntityClassName( instanceToLoad.getClass().getName() );
			return source.getEntityPersister( null, instanceToLoad );
		}
		else {
			return source.getFactory().getMappingMetamodel().getEntityDescriptor( event.getEntityClassName() );
		}
	}

	private CompletionStage<Object> doOnLoad(
			final EntityPersister persister,
			final LoadEvent event,
			final LoadType loadType) {

		final EventSource session = event.getSession();
		final EntityKey keyToLoad = session.generateEntityKey( event.getEntityId(), persister );
		if ( loadType.isNakedEntityReturned() ) {
			//do not return a proxy!
			//(this option indicates we are initializing a proxy)
			return load( event, persister, keyToLoad, loadType );
		}
		//return a proxy if appropriate
		return event.getLockMode() == LockMode.NONE
				? proxyOrLoad( event, persister, keyToLoad, loadType )
				: lockAndLoad( event, persister, keyToLoad, loadType, session );
	}

	private CompletionStage<Void> checkIdClass(
			final EntityPersister persister,
			final LoadEvent event,
			final LoadType loadType,
			final Class<?> idClass) {
		// we may have the kooky jpa requirement of allowing find-by-id where
		// "id" is the "simple pk value" of a dependent objects parent.  This
		// is part of its generally goofy "derived identity" "feature"
		// we may have the jpa requirement of allowing find-by-id where id is the "simple pk value" of a
		// dependent objects parent.  This is part of its generally goofy derived identity "feature"
		final EntityIdentifierMapping idMapping = persister.getIdentifierMapping();
		if ( idMapping instanceof CompositeIdentifierMapping ) {
			final CompositeIdentifierMapping compositeIdMapping = (CompositeIdentifierMapping) idMapping;
			final AttributeMappingsList attributeMappings = compositeIdMapping.getPartMappingType().getAttributeMappings();
			if ( attributeMappings.size() == 1 ) {
				final AttributeMapping singleIdAttribute = attributeMappings.get( 0 );
				if ( singleIdAttribute.getMappedType() instanceof EntityMappingType ) {
					final EntityMappingType parentIdTargetMapping = (EntityMappingType) singleIdAttribute.getMappedType();
					final EntityIdentifierMapping parentIdTargetIdMapping = parentIdTargetMapping.getIdentifierMapping();
					final MappingType parentIdType = parentIdTargetIdMapping instanceof CompositeIdentifierMapping
							? ((CompositeIdentifierMapping) parentIdTargetIdMapping).getMappedIdEmbeddableTypeDescriptor()
							: parentIdTargetIdMapping.getMappedType();

					if ( parentIdType.getMappedJavaType().getJavaTypeClass().isInstance( event.getEntityId() ) ) {
						// yep that's what we have...
						return loadByDerivedIdentitySimplePkValue( event, loadType, persister, compositeIdMapping, (EntityPersister) parentIdTargetMapping );
					}
				}
				else if ( idClass.isInstance( event.getEntityId() ) ) {
					return voidFuture();
				}
			}
			else if ( idMapping instanceof NonAggregatedIdentifierMapping ) {
				if ( idClass.isInstance( event.getEntityId() ) ) {
					return voidFuture();
				}
			}
		}
		throw new TypeMismatchException( "Provided id of the wrong type for class " + persister.getEntityName()
				+ ". Expected: " + idClass + ", got " + event.getEntityId().getClass() );
	}

	/*
	 * See DefaultLoadEventListener#loadByDerivedIdentitySimplePkValue
	 */
	private CompletionStage<Void> loadByDerivedIdentitySimplePkValue(
			LoadEvent event,
			LoadType options,
			EntityPersister dependentPersister,
			CompositeIdentifierMapping dependentIdType,
			EntityPersister parentPersister) {
		final EventSource session = event.getSession();
		final EntityKey parentEntityKey = session.generateEntityKey( event.getEntityId(), parentPersister );
		return doLoad( event, parentPersister, parentEntityKey, options )
				.thenApply( parent -> {
					checkEntityFound( session, parentEntityKey.getEntityName(), parentEntityKey, parent );
					final Object dependent = dependentIdType.instantiate();
					dependentIdType.getPartMappingType().setValues( dependent, new Object[] { parent } );
					event.setEntityId( dependent );
					return session.generateEntityKey( dependent, dependentPersister );
				} )
				.thenCompose( dependentEntityKey -> doLoad( event, dependentPersister, dependentEntityKey, options ) )
				.thenAccept( event::setResult );
	}

	/**
	 * Performs the load of an entity.
	 *
	 * @param event The initiating load request event
	 * @param persister The persister corresponding to the entity to be loaded
	 * @param keyToLoad The key of the entity to be loaded
	 * @param options The defined load options
	 *
	 * @return The loaded entity.
	 */
	private CompletionStage<Object> load( LoadEvent event, EntityPersister persister, EntityKey keyToLoad, LoadType options) {
		final EventSource session = event.getSession();
		if ( event.getInstanceToLoad() != null ) {
			if ( session.getPersistenceContextInternal().getEntry( event.getInstanceToLoad() ) != null ) {
				throw new PersistentObjectException(
						"attempted to load into an instance that was already associated with the session: "
								+ infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			persister.setIdentifier( event.getInstanceToLoad(), event.getEntityId(), session );
		}

		return doLoad( event, persister, keyToLoad, options )
				.thenApply( optional -> {
					boolean isOptionalInstance = event.getInstanceToLoad() != null;
					if ( optional == null && ( !options.isAllowNulls() || isOptionalInstance ) ) {
						throwEntityNotFound( session, event.getEntityClassName(), event.getEntityId() );
					}
					else if ( isOptionalInstance && optional != event.getInstanceToLoad() ) {
						throw new NonUniqueObjectException( event.getEntityId(), event.getEntityClassName() );
					}
					return optional;
				} );
	}

	/**
	 * Based on configured options, will either return a pre-existing proxy,
	 * generate a new proxy, or perform an actual load.
	 *
	 * @param event The initiating load request event
	 * @param persister The persister corresponding to the entity to be loaded
	 * @param keyToLoad The key of the entity to be loaded
	 * @param options The defined load options
	 *
	 * @return The result of the proxy/load operation.
	 */
	private CompletionStage<Object> proxyOrLoad(LoadEvent event, EntityPersister persister, EntityKey keyToLoad, LoadType options) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Loading entity: {0}",
					infoString( persister, event.getEntityId(), persister.getFactory() )
			);
		}

		// Check for the case where we can use the entity itself as a proxy
		if ( hasBytecodeProxy( persister, options ) ) {
			return loadWithBytecodeProxy( event, persister, keyToLoad, options );
		}
		else if ( persister.hasProxy() ) {
			return loadWithRegularProxy( event, persister, keyToLoad, options );
		}
		else {
			// no proxies, just return a newly loaded object
			return load( event, persister, keyToLoad, options );
		}
	}

	private static boolean wasDeleted(PersistenceContext persistenceContext, Object existing) {
		return persistenceContext.getEntry( existing ).getStatus().isDeletedOrGone();
	}

	private CompletionStage<Object> loadWithBytecodeProxy(LoadEvent event, EntityPersister persister, EntityKey keyToLoad, LoadType options) {
		// if there is already a managed entity instance associated with the PC, return it
		final EventSource session = event.getSession();
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final Object managed = persistenceContext.getEntity( keyToLoad );
		if ( managed != null ) {
			return options.isCheckDeleted() && wasDeleted( persistenceContext, managed )
					? nullFuture()
					: completedFuture( managed );
		}
		else if ( persister.getRepresentationStrategy().getProxyFactory() != null ) {
			// we have a HibernateProxy factory, this case is more complicated
			return loadWithProxyFactory( event, persister, keyToLoad );
		}
		else if ( !persister.hasSubclasses() ) {
			// the entity class has subclasses and there is no HibernateProxy factory
			return load( event, persister, keyToLoad, options );
		}
		else {
			// no HibernateProxy factory, and no subclasses
			return completedFuture( createBatchLoadableEnhancedProxy( persister, keyToLoad, session ) );
		}
	}

	private CompletionStage<Object> loadWithRegularProxy(LoadEvent event, EntityPersister persister, EntityKey keyToLoad, LoadType options) {
		// This is the case where the proxy is a separate object:
		// look for a proxy
		final Object proxy = event.getSession().getPersistenceContextInternal().getProxy( keyToLoad );
		if ( proxy != null ) {
			// narrow the existing proxy to the type we're looking for
			return narrowedProxy( event, persister, keyToLoad, options, proxy );
		}
		else if ( options.isAllowProxyCreation() ) {
			// return a new proxy
			// ORM calls DefaultLoadEventListener#proxyOrCache
			return completedFuture( proxyOrCached( event, persister, keyToLoad, options ) );
		}
		else {
			return load( event, persister, keyToLoad, options );
		}
	}

	private static boolean hasBytecodeProxy(EntityPersister persister, LoadType options) {
		return options.isAllowProxyCreation()
			&& persister.getEntityPersister().getBytecodeEnhancementMetadata().isEnhancedForLazyLoading();
	}

	private static CompletionStage<Object> loadWithProxyFactory(LoadEvent event, EntityPersister persister, EntityKey keyToLoad) {
		final EventSource session = event.getSession();
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final Object proxy = persistenceContext.getProxy( keyToLoad );

		if ( proxy != null ) {
			LOG.trace( "Entity proxy found in session cache" );
			if ( LOG.isDebugEnabled() && HibernateProxy.extractLazyInitializer( proxy ).isUnwrap() ) {
				LOG.debug( "Ignoring NO_PROXY to honor laziness" );
			}

			return completedFuture( persistenceContext.narrowProxy( proxy, persister, keyToLoad, null ) );
		}
		else if ( persister.hasSubclasses() ) {
			// specialized handling for entities with subclasses with a HibernateProxy factory
			return completedFuture( proxyOrCached( event, persister, keyToLoad ) );
		}
		else {
			// no existing proxy, and no subclasses
			return completedFuture( createBatchLoadableEnhancedProxy( persister, keyToLoad, session ) );
		}
	}

	private static PersistentAttributeInterceptable createBatchLoadableEnhancedProxy(
			EntityPersister persister,
			EntityKey keyToLoad,
			EventSource session) {
		if ( keyToLoad.isBatchLoadable() ) {
			// Add a batch-fetch entry into the queue for this entity
			session.getPersistenceContextInternal().getBatchFetchQueue().addBatchLoadableEntityKey( keyToLoad );
		}
		// This is the crux of HHH-11147
		// create the (uninitialized) entity instance - has only id set
		return persister.getBytecodeEnhancementMetadata().createEnhancedProxy( keyToLoad, true, session );
	}

	private static Object proxyOrCached(LoadEvent event, EntityPersister persister, EntityKey keyToLoad) {
		final Object cachedEntity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
				event.getSession(),
				null,
				LockMode.NONE,
				persister,
				keyToLoad
		);
		if ( cachedEntity != null ) {
			return cachedEntity;
		}
		// entities with subclasses that define a ProxyFactory can create a HibernateProxy
		return createProxy( event, persister, keyToLoad );
	}

	private static Object proxyOrCached(LoadEvent event, EntityPersister persister, EntityKey keyToLoad, LoadType options) {
		final PersistenceContext persistenceContext = event.getSession().getPersistenceContext();
		final Object existing = persistenceContext.getEntity( keyToLoad );
		if ( existing != null ) {
			return options.isCheckDeleted() && wasDeleted( persistenceContext, existing ) ? null : existing;
		}
		if ( persister.hasSubclasses() ) {
			final Object cachedEntity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
					event.getSession(),
					null,
					LockMode.NONE,
					persister,
					keyToLoad
			);
			if ( cachedEntity != null ) {
				return cachedEntity;
			}
		}
		return createProxyIfNecessary( event, persister, keyToLoad, options );
	}

	/**
	 * Given a proxy, initialize it and/or narrow it provided either
	 * is necessary.
	 *
	 * @param event The initiating load request event
	 * @param persister The persister corresponding to the entity to be loaded
	 * @param keyToLoad The key of the entity to be loaded
	 * @param options The defined load options
	 * @param proxy The proxy to narrow
	 *
	 * @return The created/existing proxy
	 */
	private CompletionStage<Object> narrowedProxy(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			LoadType options,
			Object proxy) {
		if ( LOG.isTraceEnabled() ) {
			LOG.trace( "Entity proxy found in session cache" );
		}

		LazyInitializer li = ( (HibernateProxy) proxy ).getHibernateLazyInitializer();
		if ( li.isUnwrap() ) {
			return completedFuture( li.getImplementation() );
		}
		else {
			final PersistenceContext persistenceContext = event.getSession().getPersistenceContextInternal();
			if ( options.isAllowProxyCreation() ) {
				return completedFuture( persistenceContext.narrowProxy( proxy, persister, keyToLoad, null ) );
			}
			else {
				return proxyImplementation( event, persister, keyToLoad, options )
						.thenApply( impl -> impl == null
								? null
								: persistenceContext.narrowProxy( proxy, persister, keyToLoad, impl ) );
			}
		}
	}

	private CompletionStage<Object> proxyImplementation(LoadEvent event, EntityPersister persister, EntityKey keyToLoad, LoadType options) {
		return load( event, persister, keyToLoad, options )
				.thenApply( optional -> {
					if ( optional != null ) {
						return optional;
					}
					else {
						if ( options != LoadEventListener.INTERNAL_LOAD_NULLABLE ) {
							// throw an appropriate exception
							event.getSession().getFactory().getEntityNotFoundDelegate()
									.handleEntityNotFound( persister.getEntityName(), keyToLoad.getIdentifier() );
						}
						// Otherwise, if it's INTERNAL_LOAD_NULLABLE, the proxy is
						// for a non-existing association mapped as @NotFound.
						// Don't throw an exception; just return null.
						return null;
					}
				} );
	}

	/**
	 * If there is already a corresponding proxy associated with the
	 * persistence context, return it; otherwise create a proxy, associate it
	 * with the persistence context, and return the just-created proxy.
	 *
	 * @param event The initiating load request event
	 * @param persister The persister corresponding to the entity to be loaded
	 * @param keyToLoad The key of the entity to be loaded
	 * @param options The defined load options
	 *
	 * @return The created/existing proxy
	 */
	private static Object createProxyIfNecessary(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			LoadType options) {
		final PersistenceContext persistenceContext = event.getSession().getPersistenceContextInternal();
		final Object existing = persistenceContext.getEntity( keyToLoad );
		if ( existing != null ) {
			// return existing object or initialized proxy (unless deleted)
			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Entity found in session cache" );
			}
			return options.isCheckDeleted() && wasDeleted( persistenceContext, existing ) ? null : existing;
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Creating new proxy for entity" );
			}
			return createProxy( event, persister, keyToLoad );
		}
	}

	private static Object createProxy(LoadEvent event, EntityPersister persister, EntityKey keyToLoad) {
		// return new uninitialized proxy
		final Object proxy = persister.createProxy( event.getEntityId(), event.getSession() );
		PersistenceContext persistenceContext = event.getSession().getPersistenceContextInternal();
		persistenceContext.getBatchFetchQueue().addBatchLoadableEntityKey( keyToLoad );
		persistenceContext.addProxy( keyToLoad, proxy );
		return proxy;
	}

	/**
	 * If the class to be loaded has been configured with a cache, then lock
	 * given id in that cache and then perform the load.
	 *
	 * @param event The initiating load request event
	 * @param persister The persister corresponding to the entity to be loaded
	 * @param keyToLoad The key of the entity to be loaded
	 * @param options The defined load options
	 * @param source The originating session
	 *
	 * @return The loaded entity
	 */
	private CompletionStage<Object> lockAndLoad(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			LoadType options,
			SessionImplementor source) {

		final SoftLock lock;
		final Object cacheKey;
		final boolean canWriteToCache = persister.canWriteToCache();
		if ( canWriteToCache ) {
			final EntityDataAccess cache = persister.getCacheAccessStrategy();
			cacheKey = cache.generateCacheKey(
					event.getEntityId(),
					persister,
					source.getFactory(),
					source.getTenantIdentifier()
			);
			lock = cache.lockItem( source, cacheKey, null );
		}
		else {
			cacheKey = null;
			lock = null;
		}

		try {
			return load( event, persister, keyToLoad, options )
					.whenComplete( (v, x) -> {
						if ( canWriteToCache ) {
							persister.getCacheAccessStrategy().unlockItem( source, cacheKey, lock );
						}
					} )
					.thenApply( entity -> source.getPersistenceContextInternal().proxyFor( persister, keyToLoad, entity ) );
		}
		catch (HibernateException he) {
			//in case load() throws an exception
			if ( canWriteToCache ) {
				persister.getCacheAccessStrategy().unlockItem( source, cacheKey, lock );
			}
			throw he;
		}
	}


	/**
	 * Coordinates the efforts to load a given entity.  First, an attempt is
	 * made to load the entity from the session-level cache.  If not found there,
	 * an attempt is made to locate it in second-level cache.  Lastly, an
	 * attempt is made to load it directly from the datasource.
	 *
	 * @param event The load event
	 * @param persister The persister for the entity being requested for load
	 * @param keyToLoad The EntityKey representing the entity to be loaded.
	 * @param options The load options.
	 *
	 * @return The loaded entity, or null.
	 */
	private CompletionStage<Object> doLoad(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			LoadType options) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Attempting to resolve: {0}",
					infoString( persister, event.getEntityId(), event.getSession().getFactory() )
			);
		}

		if ( event.getSession().getPersistenceContextInternal().containsDeletedUnloadedEntityKey( keyToLoad ) ) {
			return nullFuture();
		}
		else {
			final PersistenceContextEntry persistenceContextEntry =
					ReactiveCacheEntityLoaderHelper.INSTANCE.loadFromSessionCache( event, keyToLoad, options );
			final Object entity = persistenceContextEntry.getEntity();
			if ( entity != null ) {
				return persistenceContextEntry.isManaged() ? initializeIfNecessary( entity ) : nullFuture();
			}
			else {
				return loadFromCacheOrDatasource( event, persister, keyToLoad );
			}
		}
	}

	private static CompletionStage<Object> initializeIfNecessary(Object entity) {
		if ( isPersistentAttributeInterceptable( entity ) ) {
			final PersistentAttributeInterceptable interceptable = asPersistentAttributeInterceptable( entity );
			final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
			if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor) {
				final EnhancementAsProxyLazinessInterceptor lazinessInterceptor =
						(EnhancementAsProxyLazinessInterceptor) interceptor;
				final SharedSessionContractImplementor session = lazinessInterceptor.getLinkedSession();
				if ( session == null ) {
					throw LOG.sessionClosedLazyInitializationException();
				}
				return ReactiveQueryExecutorLookup.extract( session ).reactiveFetch( entity, false );
			}
			else {
				return completedFuture( entity );
			}
		}
		else {
			return completedFuture( entity );
		}
	}

	private CompletionStage<Object> loadFromCacheOrDatasource(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad) {
		final EventSource session = event.getSession();
		final Object entity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(event, persister, keyToLoad);
		if ( entity != null ) {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Resolved object in second-level cache: {0}",
						infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			cacheNaturalId( event, persister, session, entity );
			return completedFuture( entity );
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Object not resolved in any cache: {0}",
						infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			return loadFromDatasource( event, persister )
					.thenApply( optional -> {
						if ( optional != null ) {
							cacheNaturalId( event, persister, session, optional );
						}
						return optional;
					} );
		}
	}

	private void cacheNaturalId(LoadEvent event, EntityPersister persister, EventSource session, Object entity) {
		if ( entity != null && persister.hasNaturalIdentifier() ) {
			session.getPersistenceContextInternal().getNaturalIdResolutions()
					.cacheResolutionFromLoad(
							event.getEntityId(),
							persister.getNaturalIdMapping().extractNaturalIdFromEntity( entity ),
							persister
					);
		}
	}

	/**
	 * Performs the process of loading an entity from the configured
	 * underlying datasource.
	 *
	 * @param event The load event
	 * @param persister The persister for the entity being requested for load
	 *
	 * @return The object loaded from the datasource, or null if not found.
	 */
	protected CompletionStage<Object> loadFromDatasource(LoadEvent event, EntityPersister persister) {
		return ( (ReactiveEntityPersister) persister )
				.reactiveLoad(
						event.getEntityId(),
						event.getInstanceToLoad(),
						event.getLockOptions(),
						event.getSession()
				)
				.thenApply( entity -> {
					// todo (6.0) : this is a change from previous versions
					//		specifically the load call previously always returned a non-proxy
					//		so we emulate that here.  Longer term we should make the
					//		persister/loader/initializer sensitive to this fact - possibly
					//		passing LoadType along

					final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( entity );
					if ( lazyInitializer != null ) {
						entity = lazyInitializer.getImplementation();
					}

					final StatisticsImplementor statistics = event.getSession().getFactory().getStatistics();
					if ( event.isAssociationFetch() && statistics.isStatisticsEnabled() ) {
						statistics.fetchEntity( event.getEntityClassName() );
					}
					return entity;
				} );
	}
}

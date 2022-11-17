/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.session.impl.SessionUtil.throwEntityNotFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.returnNullorRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.PersistentObjectException;
import org.hibernate.TypeMismatchException;
import org.hibernate.action.internal.DelayedPostInsertIdentifier;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.loader.entity.CacheEntityLoaderHelper;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.CompositeIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.MappingType;
import org.hibernate.metamodel.mapping.NonAggregatedIdentifierMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.event.ReactiveLoadEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.tuple.entity.EntityMetamodel;

/**
 * A reactive {@link org.hibernate.event.internal.DefaultLoadEventListener}.
 * <p>
 *     Note that sometimes Hibernate ORM calls {@link org.hibernate.internal.SessionImpl#internalLoad(String, Serializable, boolean, boolean)}
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
	 * This method is not reactive but we expect it to be called only when a proxy can be returned.
	 * <p>
	 *     In particular, it should be called only by
	 *     {@link org.hibernate.internal.SessionImpl#internalLoad(String, Serializable, boolean, boolean)}.
	 * </p>
	 *
	 * @see org.hibernate.event.internal.DefaultLoadEventListener#onLoad(LoadEvent, LoadType)
	 * @throws UnsupportedOperationException if the entity loaded is not a proxy
	 * @throws UnexpectedAccessToTheDatabase if it needs to load the entity from the db
	 */
	@Override
	public void onLoad(
			final LoadEvent event,
			final LoadEventListener.LoadType loadType) throws HibernateException {
		final EntityPersister persister = getPersister( event );

		if ( persister == null ) {
			throw LOG.unableToLocatePersister( event.getEntityClassName() );
		}

		// Since this method is not reactive, we're not expecting to hit the
		// database here (if we do, it's a bug) and so we can assume the
		// returned CompletionStage is already completed
		CompletionStage<Void> checkId = checkId( event, loadType, persister );
		if ( !checkId.toCompletableFuture().isDone() ) {
			// This only happens if the object is loaded from the db
			throw new UnexpectedAccessToTheDatabase();
		}

		try {
			// Since this method is not reactive, we're not expecting to hit the
			// database here (if we do, it's a bug) and so we can assume the
			// returned CompletionStage is already completed (a proxy, perhaps)
			CompletionStage<Object> loaded = doOnLoad( persister, event, loadType );
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
	public CompletionStage<Void> reactiveOnLoad(
			final LoadEvent event,
			final LoadEventListener.LoadType loadType) throws HibernateException {

		final ReactiveEntityPersister persister = (ReactiveEntityPersister) getPersister( event );
		if ( persister == null ) {
			throw LOG.unableToLocatePersister( event.getEntityClassName() );
		}

		CompletionStage<Void> result = checkId( event, loadType, persister )
				.thenCompose( vd -> doOnLoad( persister, event, loadType )
						.thenAccept( event::setResult )
						.handle( (v, x) -> {
							if ( event.getResult() instanceof CompletionStage ) {
								throw new AssertionFailure("Unexpected CompletionStage");
							}

							if (x instanceof HibernateException) {
								LOG.unableToLoadCommand( (HibernateException) x );
							}
							return returnNullorRethrow( x );
						} )
		);

		// if a pessimistic version increment was requested, we need
		// to go back to the database immediately and update the row
		if ( event.getLockMode() == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
			// TODO: should we call CachedDomainDataAccess.lockItem() ?
			return result.thenCompose(
					v -> persister.reactiveLock(
							event.getEntityId(),
							persister.getVersion( event.getResult() ),
							event.getResult(),
							event.getLockOptions(),
							event.getSession()
					)
			);
		}

		return result;
	}

	private CompletionStage<Void> checkId(LoadEvent event, LoadType loadType, EntityPersister persister) {
		final Class<?> idClass = persister.getIdentifierType().getReturnedClass();
		if ( idClass != null
				&& !idClass.isInstance( event.getEntityId() )
				&& !(event.getEntityId() instanceof DelayedPostInsertIdentifier) ) {
			return checkIdClass( persister, event, loadType, idClass );
		}
		return voidFuture();
	}

	protected EntityPersister getPersister(final LoadEvent event) {
		final Object instanceToLoad = event.getInstanceToLoad();
		if ( instanceToLoad != null ) {
			//the load() which takes an entity does not pass an entityName
			event.setEntityClassName( instanceToLoad.getClass().getName() );
			return event.getSession().getEntityPersister( null, instanceToLoad );
		}
		else {
			return event.getSession().getFactory().getMetamodel().entityPersister( event.getEntityClassName() );
		}
	}

	private CompletionStage<Object> doOnLoad(
			final EntityPersister persister,
			final LoadEvent event,
			final LoadEventListener.LoadType loadType) {

		final EventSource session = event.getSession();
		final EntityKey keyToLoad = session.generateEntityKey( event.getEntityId(), persister );
		if ( loadType.isNakedEntityReturned() ) {
			//do not return a proxy!
			//(this option indicates we are initializing a proxy)
			return load( event, persister, keyToLoad, loadType );
		}
		//return a proxy if appropriate
		else if ( event.getLockMode() == LockMode.NONE ) {
			return proxyOrLoad( event, persister, keyToLoad, loadType );
		}
		else {
			return lockAndLoad( event, persister, keyToLoad, loadType, session );
		}
	}

	private CompletionStage<Void> checkIdClass(
			final EntityPersister persister,
			final LoadEvent event,
			final LoadEventListener.LoadType loadType,
			final Class<?> idClass) {
		// we may have the kooky jpa requirement of allowing find-by-id where
		// "id" is the "simple pk value" of a dependent objects parent.  This
		// is part of its generally goofy "derived identity" "feature"
		// we may have the jpa requirement of allowing find-by-id where id is the "simple pk value" of a
		// dependent objects parent.  This is part of its generally goofy derived identity "feature"
		final EntityIdentifierMapping idMapping = persister.getIdentifierMapping();
		if ( idMapping instanceof CompositeIdentifierMapping ) {
			final CompositeIdentifierMapping compositeIdMapping = (CompositeIdentifierMapping) idMapping;
			final List<AttributeMapping> attributeMappings = compositeIdMapping.getPartMappingType().getAttributeMappings();
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
						return loadByDerivedIdentitySimplePkValue(
								event,
								loadType,
								persister,
								compositeIdMapping,
								(EntityPersister) parentIdTargetMapping
						);
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
		throw new TypeMismatchException(
				"Provided id of the wrong type for class " + persister.getEntityName() + ". Expected: " + idClass + ", got " + event.getEntityId().getClass() );
	}

	/*
	 * See DefaultLoadEventListener#loadByDerivedIdentitySimplePkValue
	 */
	private CompletionStage<Void> loadByDerivedIdentitySimplePkValue(LoadEvent event, LoadEventListener.LoadType options,
			EntityPersister dependentPersister, CompositeIdentifierMapping dependentIdType, EntityPersister parentPersister) {
		EventSource session = event.getSession();
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
					if ( optional==null && ( !options.isAllowNulls() || isOptionalInstance ) ) {
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
	private CompletionStage<Object> proxyOrLoad(LoadEvent event,
												EntityPersister persister,
												EntityKey keyToLoad,
												LoadEventListener.LoadType options) {
		final EventSource session = event.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		final boolean traceEnabled = LOG.isTraceEnabled();

		if ( traceEnabled ) {
			LOG.tracev(
					"Loading entity: {0}",
					infoString( persister, event.getEntityId(), factory )
			);
		}

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();

		final EntityMetamodel entityMetamodel = persister.getEntityMetamodel();

		// Check for the case where we can use the entity itself as a proxy
		if ( options.isAllowProxyCreation()
				&& entityMetamodel.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading() ) {
			// if there is already a managed entity instance associated with the PC, return it
			final Object managed = persistenceContext.getEntity( keyToLoad );
			if ( managed != null ) {
				if ( options.isCheckDeleted() ) {
					final EntityEntry entry = persistenceContext.getEntry( managed );
					final Status status = entry.getStatus();
					if ( status == Status.DELETED || status == Status.GONE ) {
						return nullFuture();
					}
				}
				return completedFuture( managed );
			}

			// if the entity defines a HibernateProxy factory, see if there is an
			// existing proxy associated with the PC - and if so, use it
			if ( persister.getRepresentationStrategy().getProxyFactory() != null ) {
				final Object proxy = persistenceContext.getProxy( keyToLoad );

				if ( proxy != null ) {
					if( traceEnabled ) {
						LOG.trace( "Entity proxy found in session cache" );
					}

					if ( LOG.isDebugEnabled() && ( (HibernateProxy) proxy ).getHibernateLazyInitializer().isUnwrap() ) {
						LOG.debug( "Ignoring NO_PROXY to honor laziness" );
					}

					return completedFuture( persistenceContext.narrowProxy( proxy, persister, keyToLoad, null ) );
				}

				// specialized handling for entities with subclasses with a HibernateProxy factory
				if ( entityMetamodel.hasSubclasses() ) {
					// entities with subclasses that define a ProxyFactory can create a HibernateProxy
					return completedFuture( createProxy( event, persister, keyToLoad, persistenceContext ) );
				}
			}

			if ( !entityMetamodel.hasSubclasses() ) {
				if ( keyToLoad.isBatchLoadable() ) {
					// Add a batch-fetch entry into the queue for this entity
					persistenceContext.getBatchFetchQueue().addBatchLoadableEntityKey( keyToLoad );
				}

				// This is the crux of HHH-11147
				// create the (uninitialized) entity instance - has only id set
				return completedFuture( persister.getBytecodeEnhancementMetadata()
						.createEnhancedProxy( keyToLoad, true, session ) );
			}

			// If we get here, then the entity class has subclasses and there is no HibernateProxy factory.
			// The entity will get loaded below. );
		}
		else {
			if ( persister.hasProxy() ) {
				// look for a proxy
				Object proxy = persistenceContext.getProxy( keyToLoad );
				if ( proxy != null ) {
					return returnNarrowedProxy( event, persister, keyToLoad, options, persistenceContext, proxy );
				}

				if ( options.isAllowProxyCreation() ) {
					return completedFuture( createProxyIfNecessary( event, persister, keyToLoad, options, persistenceContext ) );
				}
			}
		}

		// return a newly loaded object
		return load( event, persister, keyToLoad, options );
	}


	/**
	 * Given a proxy, initialize it and/or narrow it provided either
	 * is necessary.
	 *
	 * @param event The initiating load request event
	 * @param persister The persister corresponding to the entity to be loaded
	 * @param keyToLoad The key of the entity to be loaded
	 * @param options The defined load options
	 * @param persistenceContext The originating session
	 * @param proxy The proxy to narrow
	 *
	 * @return The created/existing proxy
	 */
	private CompletionStage<Object> returnNarrowedProxy(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			LoadEventListener.LoadType options,
			PersistenceContext persistenceContext,
			Object proxy) {
		if ( LOG.isTraceEnabled() ) {
			LOG.trace( "Entity proxy found in session cache" );
		}

		LazyInitializer li = ( (HibernateProxy) proxy ).getHibernateLazyInitializer();

		if ( li.isUnwrap() ) {
			return completedFuture( li.getImplementation() );
		}

		CompletionStage<Object> implStage;
		if ( !options.isAllowProxyCreation() ) {
			implStage = load( event, persister, keyToLoad, options )
					.thenApply( optional -> {
						if ( optional == null ) {
							event.getSession()
									.getFactory()
									.getEntityNotFoundDelegate()
									.handleEntityNotFound( persister.getEntityName(), keyToLoad.getIdentifier() );
						}
						return optional;
					} );
		}
		else {
			implStage = nullFuture();
		}

		return implStage.thenApply( impl -> persistenceContext.narrowProxy( proxy, persister, keyToLoad, impl ) );
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
	 * @param persistenceContext The originating session
	 *
	 * @return The created/existing proxy
	 */
	private Object createProxyIfNecessary(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			LoadEventListener.LoadType options,
			PersistenceContext persistenceContext) {
		Object existing = persistenceContext.getEntity( keyToLoad );
		final boolean traceEnabled = LOG.isTraceEnabled();
		if ( existing != null ) {
			// return existing object or initialized proxy (unless deleted)
			if ( traceEnabled ) {
				LOG.trace( "Entity found in session cache" );
			}
			if ( options.isCheckDeleted() ) {
				EntityEntry entry = persistenceContext.getEntry( existing );
				Status status = entry.getStatus();
				if ( status == Status.DELETED || status == Status.GONE ) {
					return null;
				}
			}
			return existing;
		}
		if ( traceEnabled ) {
			LOG.trace( "Creating new proxy for entity" );
		}
		return createProxy( event, persister, keyToLoad, persistenceContext );
	}

	private Object createProxy(
			LoadEvent event,
			EntityPersister persister,
			EntityKey keyToLoad,
			PersistenceContext persistenceContext) {
		// return new uninitialized proxy
		Object proxy = persister.createProxy( event.getEntityId(), event.getSession() );
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
			LoadEventListener.LoadType options,
			SessionImplementor source) {

		final boolean canWriteToCache = persister.canWriteToCache();
		final SoftLock lock;
		final Object cacheKey;
		if ( canWriteToCache ) {
			EntityDataAccess cache = persister.getCacheAccessStrategy();
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
			LoadEventListener.LoadType options) {

		final EventSource session = event.getSession();
		final boolean traceEnabled = LOG.isTraceEnabled();
		if ( traceEnabled ) {
			LOG.tracev(
					"Attempting to resolve: {0}",
					infoString( persister, event.getEntityId(), session.getFactory() )
			);
		}

		CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry = CacheEntityLoaderHelper.INSTANCE
				.loadFromSessionCache( event, keyToLoad, options );
		Object entity = persistenceContextEntry.getEntity();
		if ( entity != null ) {
			return persistenceContextEntry.isManaged()
					? completedFuture( entity )
					: nullFuture();
		}

		entity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache( event, persister, keyToLoad );
		if ( entity != null ) {
			if ( traceEnabled ) {
				LOG.tracev(
						"Resolved object in second-level cache: {0}",
						infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			cacheNaturalId( event, persister, session, entity );
			return completedFuture( entity );
		}
		else {
			if ( traceEnabled ) {
				LOG.tracev(
						"Object not resolved in any cache: {0}",
						infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			return loadFromDatasource( event, persister )
					.thenApply( optional -> {
						if ( optional!=null ) {
							cacheNaturalId( event, persister, session, optional );
						}
						return optional;
					} );
		}
	}

	private void cacheNaturalId(LoadEvent event, EntityPersister persister, EventSource session, Object entity) {
		if ( entity != null && persister.hasNaturalIdentifier() ) {
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			persistenceContext.getNaturalIdResolutions()
					.cacheResolutionFromLoad( event.getEntityId(),
											  persister.getNaturalIdMapping().extractNaturalIdFromEntity( entity, session ),
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

					if ( entity instanceof HibernateProxy ) {
						entity = ( (HibernateProxy) entity ).getHibernateLazyInitializer().getImplementation();
					}

					final StatisticsImplementor statistics = event.getSession().getFactory().getStatistics();
					if ( event.isAssociationFetch() && statistics.isStatisticsEnabled() ) {
						statistics.fetchEntity( event.getEntityClassName() );
					}
					return entity;
				} );
	}
}

package org.hibernate.rx.event.impl;

import org.hibernate.*;
import org.hibernate.action.internal.DelayedPostInsertIdentifier;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.*;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.loader.entity.CacheEntityLoaderHelper;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.rx.event.spi.RxLoadEventListener;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.tuple.IdentifierProperty;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.EmbeddedComponentType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultLoadEventListener}.
 */
public class DefaultRxLoadEventListener implements LoadEventListener, RxLoadEventListener {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxLoadEventListener.class );

	/**
	 * Handle the given load event.
	 *
	 * @param event The load event to be handled.
	 */
	public void onLoad(
			final LoadEvent event,
			final LoadEventListener.LoadType loadType) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Handle the given load event.
	 *
	 * @param event The load event to be handled.
	 */
	public CompletionStage<Void> rxOnLoad(
			final LoadEvent event,
			final LoadEventListener.LoadType loadType) throws HibernateException {

		final EntityPersister persister = getPersister( event );

		if ( persister == null ) {
			throw new HibernateException( "Unable to locate persister: " + event.getEntityClassName() );
		}

		final Class<?> idClass = persister.getIdentifierType().getReturnedClass();
		if ( idClass != null &&
				!idClass.isInstance( event.getEntityId() ) &&
				!(event.getEntityId() instanceof DelayedPostInsertIdentifier)) {
			// FIXME (Stef): this calls setResult in some cases, but the original code went on to call doOnLoad
			// I wonder if it was a bug or if I should go on too?
			return checkIdClass( persister, event, loadType, idClass );
		}

		return doOnLoad( persister, event, loadType );
	}

	protected EntityPersister getPersister(final LoadEvent event) {
		final Object instanceToLoad = event.getInstanceToLoad();
		if ( instanceToLoad != null ) {
			//the load() which takes an entity does not pass an entityName
			event.setEntityClassName( instanceToLoad.getClass().getName() );
			return event.getSession().getEntityPersister(
					null,
					instanceToLoad
			);
		}
		else {
			return event.getSession().getFactory().getMetamodel().entityPersister( event.getEntityClassName() );
		}
	}

	private CompletionStage<Void> doOnLoad(
			final EntityPersister persister,
			final LoadEvent event,
			final LoadEventListener.LoadType loadType) {

		final EventSource session = event.getSession();
		final EntityKey keyToLoad = session.generateEntityKey( event.getEntityId(), persister );
		CompletionStage<Object> ret;
		if ( loadType.isNakedEntityReturned() ) {
			//do not return a proxy!
			//(this option indicates we are initializing a proxy)
			ret = load( event, persister, keyToLoad, loadType );
		}
		else {
			//return a proxy if appropriate
			if ( event.getLockMode() == LockMode.NONE ) {
				ret = proxyOrLoad( event, persister, keyToLoad, loadType );
			}
			else {
				ret = lockAndLoad( event, persister, keyToLoad, loadType, session );
			}
		}
		assert ret != null;
		return ret.handle( (res, x) -> {
			if ( x instanceof HibernateException ) {
				LOG.unableToLoadCommand( (HibernateException) x);
			}
			if ( x != null ) {
				RxUtil.rethrow( x );
			}
			event.setResult( res );
			return null;
		} );

	}

	private CompletionStage<Void> checkIdClass(
			final EntityPersister persister,
			final LoadEvent event,
			final LoadEventListener.LoadType loadType,
			final Class<?> idClass) {
		// we may have the kooky jpa requirement of allowing find-by-id where
		// "id" is the "simple pk value" of a dependent objects parent.  This
		// is part of its generally goofy "derived identity" "feature"
		final IdentifierProperty identifierProperty = persister.getEntityMetamodel().getIdentifierProperty();
		if ( identifierProperty.isEmbedded() ) {
			final EmbeddedComponentType dependentIdType =
					(EmbeddedComponentType) identifierProperty.getType();
			if ( dependentIdType.getSubtypes().length == 1 ) {
				final Type singleSubType = dependentIdType.getSubtypes()[0];
				if ( singleSubType.isEntityType() ) {
					final EntityType dependentParentType = (EntityType) singleSubType;
					final SessionFactoryImplementor factory = event.getSession().getFactory();
					final Type dependentParentIdType = dependentParentType.getIdentifierOrUniqueKeyType( factory );
					if ( dependentParentIdType.getReturnedClass().isInstance( event.getEntityId() ) ) {
						// yep that's what we have...
						return loadByDerivedIdentitySimplePkValue(
								event,
								loadType,
								persister,
								dependentIdType,
								factory.getMetamodel().entityPersister( dependentParentType.getAssociatedEntityName() )
						);
					}
				}
			}
		}
		throw new TypeMismatchException(
				"Provided id of the wrong type for class " + persister.getEntityName() + ". Expected: " + idClass
						+ ", got " + event.getEntityId().getClass()
		);
	}

	private CompletionStage<Void> loadByDerivedIdentitySimplePkValue(
			LoadEvent event,
			LoadEventListener.LoadType options,
			EntityPersister dependentPersister,
			EmbeddedComponentType dependentIdType,
			EntityPersister parentPersister) {
		final EventSource session = event.getSession();
		final EntityKey parentEntityKey = session.generateEntityKey( event.getEntityId(), parentPersister );
		return doLoad( event, parentPersister, parentEntityKey, options )
				.thenCompose( parent -> {
					final Serializable dependent = (Serializable) dependentIdType.instantiate( parent, session );
					dependentIdType.setPropertyValues(
							dependent,
							new Object[] { parent },
							dependentPersister.getEntityMode()
					);
					final EntityKey dependentEntityKey = session.generateEntityKey( dependent, dependentPersister );
					event.setEntityId( dependent );

					return doLoad( event, dependentPersister, dependentEntityKey, options ).thenAccept(event::setResult);
				} );
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
	private CompletionStage<Object> load(
			final LoadEvent event,
			final EntityPersister persister,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options) {

		final EventSource session = event.getSession();
		if ( event.getInstanceToLoad() != null ) {
			if ( session.getPersistenceContextInternal().getEntry( event.getInstanceToLoad() ) != null ) {
				throw new PersistentObjectException(
						"attempted to load into an instance that was already associated with the session: " +
								MessageHelper.infoString(
										persister,
										event.getEntityId(),
										session.getFactory()
								)
				);
			}
			persister.setIdentifier( event.getInstanceToLoad(), event.getEntityId(), session );
		}

		final CompletionStage<Optional<Object>> entityCs = doLoad( event, persister, keyToLoad, options );

		return entityCs.thenApply( optional -> {
			boolean isOptionalInstance = event.getInstanceToLoad() != null;

			if ( !optional.isPresent() && ( !options.isAllowNulls() || isOptionalInstance ) ) {
				session
						.getFactory()
						.getEntityNotFoundDelegate()
						.handleEntityNotFound( event.getEntityClassName(), event.getEntityId() );
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
	private CompletionStage<Object> proxyOrLoad(
			final LoadEvent event,
			final EntityPersister persister,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options) {

		final EventSource session = event.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		final boolean traceEnabled = LOG.isTraceEnabled();

		if ( traceEnabled ) {
			LOG.tracev(
					"Loading entity: {0}",
					MessageHelper.infoString( persister, event.getEntityId(), factory )
			);
		}

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();

		final boolean allowBytecodeProxy = factory
				.getSessionFactoryOptions()
				.isEnhancementAsProxyEnabled();

		final EntityMetamodel entityMetamodel = persister.getEntityMetamodel();
		final boolean entityHasHibernateProxyFactory = entityMetamodel
				.getTuplizer()
				.getProxyFactory() != null;

		// Check for the case where we can use the entity itself as a proxy
		if ( options.isAllowProxyCreation()
				&& allowBytecodeProxy
				&& entityMetamodel.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading() ) {
			// if there is already a managed entity instance associated with the PC, return it
			final Object managed = persistenceContext.getEntity( keyToLoad );
			if ( managed != null ) {
				if ( options.isCheckDeleted() ) {
					final EntityEntry entry = persistenceContext.getEntry( managed );
					final Status status = entry.getStatus();
					if ( status == Status.DELETED || status == Status.GONE ) {
						return null;
					}
				}
				return RxUtil.completedFuture( managed );
			}

			// if the entity defines a HibernateProxy factory, see if there is an
			// existing proxy associated with the PC - and if so, use it
			if ( entityHasHibernateProxyFactory ) {
				final Object proxy = persistenceContext.getProxy( keyToLoad );

				if ( proxy != null ) {
					if( traceEnabled ) {
						LOG.trace( "Entity proxy found in session cache" );
					}

					if ( LOG.isDebugEnabled() && ( (HibernateProxy) proxy ).getHibernateLazyInitializer().isUnwrap() ) {
						LOG.debug( "Ignoring NO_PROXY to honor laziness" );
					}

					return RxUtil.completedFuture( persistenceContext.narrowProxy( proxy, persister, keyToLoad, null ) );
				}

				// specialized handling for entities with subclasses with a HibernateProxy factory
				if ( entityMetamodel.hasSubclasses() ) {
					// entities with subclasses that define a ProxyFactory can create a HibernateProxy
					return RxUtil.completedFuture( createProxy( event, persister, keyToLoad, persistenceContext ) );
				}
			}

			if ( !entityMetamodel.hasSubclasses() ) {
				if ( keyToLoad.isBatchLoadable() ) {
					// Add a batch-fetch entry into the queue for this entity
					persistenceContext.getBatchFetchQueue().addBatchLoadableEntityKey( keyToLoad );
				}

				// This is the crux of HHH-11147
				// create the (uninitialized) entity instance - has only id set
				return RxUtil.completedFuture( persister.getBytecodeEnhancementMetadata().createEnhancedProxy( keyToLoad, true, session ) );
			}

			// If we get here, then the entity class has subclasses and there is no HibernateProxy factory.
			// The entity will get loaded below. );
		}
		else {
			if ( persister.hasProxy() ) {
				// look for a proxy
				Object proxy = persistenceContext.getProxy( keyToLoad );
				if ( proxy != null ) {
					return RxUtil.completedFuture( returnNarrowedProxy(
							event,
							persister,
							keyToLoad,
							options,
							persistenceContext,
							proxy
					) );
				}

				// FIXME: We are skipping proxies for now.
				// 		  Maybe we should make this return false instead of commenting.
//				if ( options.isAllowProxyCreation() ) {
//					return RxUtil.completedFuture( createProxyIfNecessary(
//							event,
//							persister,
//							keyToLoad,
//							options,
//							persistenceContext
//					) );
//				}
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
	private Object returnNarrowedProxy(
			final LoadEvent event,
			final EntityPersister persister,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options,
			final PersistenceContext persistenceContext,
			final Object proxy) {
		if ( LOG.isTraceEnabled() ) {
			LOG.trace( "Entity proxy found in session cache" );
		}

		LazyInitializer li = ( (HibernateProxy) proxy ).getHibernateLazyInitializer();

		if ( li.isUnwrap() ) {
			return li.getImplementation();
		}

		Object impl = null;
		if ( !options.isAllowProxyCreation() ) {
			impl = load( event, persister, keyToLoad, options );
			if ( impl == null ) {
				event.getSession()
						.getFactory()
						.getEntityNotFoundDelegate()
						.handleEntityNotFound( persister.getEntityName(), keyToLoad.getIdentifier() );
			}
		}

		return persistenceContext.narrowProxy( proxy, persister, keyToLoad, impl );
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
			final LoadEvent event,
			final EntityPersister persister,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options,
			final PersistenceContext persistenceContext) {
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
			final LoadEvent event,
			final EntityPersister persister,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options,
			final SessionImplementor source) {
		SoftLock lock = null;
		final Object ck;
		final EntityDataAccess cache = persister.getCacheAccessStrategy();
		final boolean canWriteToCache = persister.canWriteToCache();
		if ( canWriteToCache ) {
			ck = cache.generateCacheKey(
					event.getEntityId(),
					persister,
					source.getFactory(),
					source.getTenantIdentifier()
			);
			lock = cache.lockItem( source, ck, null );
		}
		else {
			ck = null;
		}

		SoftLock finalLock = lock;
		return load( event, persister, keyToLoad, options )
				.whenComplete( (v, x) -> {
					if ( canWriteToCache ) {
						cache.unlockItem( source, ck, finalLock );
					}
				} ).thenApply( entity -> source.getPersistenceContextInternal()
						.proxyFor( persister, keyToLoad, entity ) );
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
	private CompletionStage<Optional<Object>> doLoad(
			final LoadEvent event,
			final EntityPersister persister,
			final EntityKey keyToLoad,
			final LoadEventListener.LoadType options) {

		final EventSource session = event.getSession();
		final boolean traceEnabled = LOG.isTraceEnabled();
		if ( traceEnabled ) {
			LOG.tracev(
					"Attempting to resolve: {0}",
					MessageHelper.infoString( persister, event.getEntityId(), session.getFactory() )
			);
		}

		CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry = CacheEntityLoaderHelper.INSTANCE.loadFromSessionCache(
				event,
				keyToLoad,
				options
		);
		Object entity = persistenceContextEntry.getEntity();
		if ( entity != null ) {
			return RxUtil.completedFuture( persistenceContextEntry.isManaged() ? Optional.of( entity ) : Optional.empty() );
		}

		entity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache( event, persister, keyToLoad );
		if ( entity != null ) {
			if ( traceEnabled ) {
				LOG.tracev(
						"Resolved object in second-level cache: {0}",
						MessageHelper.infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			cacheNaturalId( event, persister, session, entity );
			return RxUtil.completedFuture( Optional.of( entity ) );
		}
		else {
			if ( traceEnabled ) {
				LOG.tracev(
						"Object not resolved in any cache: {0}",
						MessageHelper.infoString( persister, event.getEntityId(), session.getFactory() )
				);
			}
			CompletionStage<Optional<Object>> entityStage = loadFromDatasource( event, persister );
			return entityStage.thenApply( optional -> {
				if ( optional.isPresent() ) {
					cacheNaturalId( event, persister, session, optional.get() );
				}
				return optional;
			} );
		}
	}

	private void cacheNaturalId(LoadEvent event, EntityPersister persister, EventSource session, Object entity) {
		if ( entity != null && persister.hasNaturalIdentifier() ) {
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			final PersistenceContext.NaturalIdHelper naturalIdHelper = persistenceContext.getNaturalIdHelper();
			naturalIdHelper.cacheNaturalIdCrossReferenceFromLoad(
					persister,
					event.getEntityId(),
					naturalIdHelper.extractNaturalIdValues(
							entity,
							persister
					)
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
	@SuppressWarnings("unchecked")
	protected CompletionStage<Optional<Object>> loadFromDatasource(
			final LoadEvent event,
			final EntityPersister persister) {

		Object entity = persister.load(
				event.getEntityId(),
				event.getInstanceToLoad(),
				event.getLockOptions(),
				event.getSession()
		);

		final StatisticsImplementor statistics = event.getSession().getFactory().getStatistics();
		if ( event.isAssociationFetch() && statistics.isStatisticsEnabled() ) {
			statistics.fetchEntity( event.getEntityClassName() );
		}

		// This should work because we are using the rx persister
		return (CompletionStage<Optional<Object>>) entity;
	}

}

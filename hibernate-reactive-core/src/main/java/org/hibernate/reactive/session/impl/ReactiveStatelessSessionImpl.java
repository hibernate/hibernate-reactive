/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.UnresolvableObjectException;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.internal.RootGraphImpl;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.StatelessSessionImpl;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveStatelessSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.spi.TypeConfiguration;

import jakarta.persistence.EntityGraph;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.assignIdIfNecessary;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.generateId;
import static org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister.forceInitialize;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

/**
 * An {@link ReactiveStatelessSession} implemented by extension of
 * the {@link StatelessSessionImpl} in Hibernate core. Extension was
 * preferred to delegation because there are places where
 * Hibernate core compares the identity of session instances.
 */
public class ReactiveStatelessSessionImpl extends StatelessSessionImpl
		implements ReactiveStatelessSession {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveConnection reactiveConnection;

	private final ReactiveStatelessSession batchingHelperSession;

	private final PersistenceContext persistenceContext;

	public ReactiveStatelessSessionImpl(
			SessionFactoryImpl factory,
			SessionCreationOptions options,
			ReactiveConnection connection) {
		super( factory, options );
		reactiveConnection = connection;
		persistenceContext = new ReactivePersistenceContextAdapter( this );
		batchingHelperSession = new ReactiveStatelessSessionImpl( factory, options, reactiveConnection, persistenceContext );
	}

	/**
	 * Create a helper instance with an underling {@link BatchingConnection}
	 */
	private ReactiveStatelessSessionImpl(
			SessionFactoryImpl factory,
			SessionCreationOptions options,
			ReactiveConnection connection,
			PersistenceContext persistenceContext) {
		super( factory, options );
		this.persistenceContext = persistenceContext;
		Integer batchSize = getConfiguredJdbcBatchSize();
		reactiveConnection = batchSize == null || batchSize < 2
				? connection
				: new BatchingConnection( connection, batchSize );
		batchingHelperSession = this;
	}

	private LockOptions getNullSafeLockOptions(LockMode lockMode) {
		return new LockOptions( lockMode == null ? LockMode.NONE : lockMode );
	}

	@Override
	public TypeConfiguration getTypeConfiguration() {
		return super.getTypeConfiguration();
	}

	@Override
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}

	@Override
	public SharedSessionContractImplementor getSession() {
		return super.getSession();
	}

	@Override
	public void checkOpen() {
		super.checkOpen();
	}

	@Override
	public Dialect getDialect() {
		return getJdbcServices().getDialect();
	}

	@Override
	public SharedSessionContractImplementor getSharedContract() {
		return this;
	}

	@Override
	public PersistenceContext getPersistenceContextInternal() {
		return persistenceContext;
	}

	@Override
	public boolean isEnforcingFetchGraph() {
		return super.isEnforcingFetchGraph();
	}

	@Override
	public void setEnforcingFetchGraph(boolean enforcingFetchGraph) {
		super.setEnforcingFetchGraph( enforcingFetchGraph );
	}

	@Override
	public ReactiveConnection getReactiveConnection() {
		return reactiveConnection;
	}

	@Override
	public void checkTransactionNeededForUpdateOperation(String exceptionMessage) {
		//no-op because we don't support transactions
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(Class<? extends T> entityClass, Object id) {
		return reactiveGet( entityClass.getName(), id, LockMode.NONE, null );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(String entityName, Object id) {
		return reactiveGet( entityName, id, LockMode.NONE, null );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(Class<? extends T> entityClass, Object id, LockMode lockMode, EntityGraph<T> fetchGraph) {
		return reactiveGet( entityClass.getName(), id, LockMode.NONE, fetchGraph );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(String entityName, Object id, LockMode lockMode, EntityGraph<T> fetchGraph) {
		checkOpen();

		if ( fetchGraph != null ) {
			getLoadQueryInfluencers()
					.getEffectiveEntityGraph()
					.applyGraph( (RootGraphImplementor<T>) fetchGraph, GraphSemantic.FETCH );
		}

		final ReactiveEntityPersister entityDescriptor = (ReactiveEntityPersister) getFactory().getRuntimeMetamodels()
				.getMappingMetamodel()
				.getEntityDescriptor( entityName );

		LockOptions lockOptions = getNullSafeLockOptions( lockMode );
		return entityDescriptor
				.reactiveLoad( id, null, lockOptions, this )
				.whenComplete( (v, e) -> {
					if ( getPersistenceContext().isLoadFinished() ) {
						getPersistenceContext().clear();
					}
					getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
				} )
				.thenApply( entity -> (T) entity );
	}

	@Override
	public ReactiveEntityPersister getEntityPersister(String entityName, Object object) throws HibernateException {
		return (ReactiveEntityPersister) super.getEntityPersister( entityName, object );
	}

	@Override
	public CompletionStage<Void> reactiveInsert(Object entity) {
		checkOpen();
		ReactiveEntityPersister persister = getEntityPersister( null, entity );
		return generateId( entity, persister, this, this )
				.thenCompose( id -> {
					Object[] state = persister.getPropertyValues( entity );
					if ( persister.isVersioned() ) {
						boolean substitute = Versioning.seedVersion(
								state,
								persister.getVersionProperty(),
								persister.getVersionMapping(),
								this
						);
						if ( substitute ) {
							persister.setValues( entity, state );
						}
					}

					if ( persister.isIdentifierAssignedByInsert() ) {
						return persister.insertReactive( state, entity, this )
								.thenAccept( generatedId -> assignIdIfNecessary(
										entity,
										generatedId,
										persister,
										this
								) );
					}
					else {
						id = assignIdIfNecessary( id, entity, persister, this );
						persister.setIdentifier( entity, id, this );
						return persister.insertReactive( id, state, entity, this );
					}
				} );
	}

	@Override
	public CompletionStage<Void> reactiveDelete(Object entity) {
		checkOpen();
		ReactiveEntityPersister persister = getEntityPersister( null, entity );
		Object id = persister.getIdentifier( entity, this );
		Object version = persister.getVersion( entity );
		return persister.deleteReactive( id, version, entity, this );
	}

	@Override
	public CompletionStage<Void> reactiveUpdate(Object entity) {
		checkOpen();
		if ( entity instanceof HibernateProxy ) {
			final LazyInitializer hibernateLazyInitializer = ( (HibernateProxy) entity ).getHibernateLazyInitializer();
			return hibernateLazyInitializer.isUninitialized()
					? failedFuture( LOG.uninitializedProxyUpdate( entity.getClass() ) )
					: executeReactiveUpdate( hibernateLazyInitializer.getImplementation() );
		}

		return executeReactiveUpdate( entity );
	}

	/**
	 * @param entity a detached entity or initialized proxy
	 *
	 * @return a void stage
	 */
	private CompletionStage<Void> executeReactiveUpdate(Object entity) {
		ReactiveEntityPersister persister = getEntityPersister( null, entity );
		Object id = persister.getIdentifier( entity, this );
		Object[] state = persister.getValues( entity );
		Object oldVersion;
		if ( persister.isVersioned() ) {
			oldVersion = persister.getVersion( entity );
			Object newVersion = Versioning.increment( oldVersion, persister.getVersionMapping(), this );
			Versioning.setVersion( state, newVersion, persister );
			persister.setValues( entity, state );
		}
		else {
			oldVersion = null;
		}
		return persister.updateReactive( id, state, null, false, null, oldVersion, entity, null, this );
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object entity) {
		return reactiveRefresh( entity, LockMode.NONE );
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode) {
		final ReactiveEntityPersister persister = getEntityPersister( null, entity );
		final Object id = persister.getIdentifier( entity, this );

		if ( persister.canWriteToCache() ) {
			final EntityDataAccess cacheAccess = persister.getCacheAccessStrategy();
			if ( cacheAccess != null ) {
				final Object ck = cacheAccess.generateCacheKey(
						id,
						persister,
						getFactory(),
						getTenantIdentifier()
				);
				cacheAccess.evict( ck );
			}
		}

		String previousFetchProfile = getLoadQueryInfluencers().getInternalFetchProfile();
		getLoadQueryInfluencers().setInternalFetchProfile( "refresh" );
		return persister.reactiveLoad( id, entity, getNullSafeLockOptions( lockMode ), this )
				.thenAccept( result -> {
					if ( getPersistenceContext().isLoadFinished() ) {
						getPersistenceContext().clear();
					}
					UnresolvableObjectException.throwIfNull( result, id, persister.getEntityName() );
				} )
				.whenComplete( (v, e) -> getLoadQueryInfluencers().setInternalFetchProfile( previousFetchProfile ) );
	}

	@Override
	public CompletionStage<Void> reactiveInsertAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveInsert )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveInsertAll(int batchSize, Object... entities) {
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveInsert )
				.thenCompose( v -> connection.executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveUpdate )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateAll(int batchSize, Object... entities) {
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveUpdate )
				.thenCompose( v -> connection.executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveDelete )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAll(int batchSize, Object... entities) {
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveDelete )
				.thenCompose( v -> connection.executeBatch() );
	}


	@Override
	public CompletionStage<Void> reactiveRefreshAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveRefresh )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveRefreshAll(int batchSize, Object... entities) {
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveRefresh )
				.thenCompose( v -> connection.executeBatch() );
	}

	private ReactiveConnection batchingConnection(int batchSize) {
		return batchingHelperSession.getReactiveConnection()
				.withBatchSize( batchSize );
	}

	private Object createProxy(EntityKey entityKey) {
		final Object proxy = entityKey.getPersister().createProxy( entityKey.getIdentifier(), this );
		getPersistenceContext().addProxy( entityKey, proxy );
		return proxy;
	}

	@Override
	public CompletionStage<Object> reactiveInternalLoad(
			String entityName,
			Object id,
			boolean eager,
			boolean nullable) {
		checkOpen();

		final EntityPersister entityDescriptor = getFactory().getRuntimeMetamodels()
				.getMappingMetamodel()
				.getEntityDescriptor( entityName );
		final EntityKey entityKey = generateEntityKey( id, entityDescriptor );

		// first, try to load it from the temp PC associated to this SS
		PersistenceContext persistenceContext = getPersistenceContext();
		Object loaded = persistenceContext.getEntity( entityKey );
		if ( loaded != null ) {
			// we found it in the temp PC.  Should indicate we are in the midst of processing a result set
			// containing eager fetches via join fetch
			return completedFuture( loaded );
		}

		if ( !eager ) {
			// caller did not request forceful eager loading, see if we can create
			// some form of proxy

			// first, check to see if we can use "bytecode proxies"

			EntityMetamodel entityMetamodel = entityDescriptor.getEntityMetamodel();
			BytecodeEnhancementMetadata enhancementMetadata = entityMetamodel.getBytecodeEnhancementMetadata();
			if ( enhancementMetadata.isEnhancedForLazyLoading() ) {

				// if the entity defines a HibernateProxy factory, see if there is an
				// existing proxy associated with the PC - and if so, use it
				if ( entityDescriptor.getRepresentationStrategy().getProxyFactory() != null ) {
					final Object proxy = persistenceContext.getProxy( entityKey );

					if ( proxy != null ) {
                        if ( LOG.isTraceEnabled() ) {
                            LOG.trace( "Entity proxy found in session cache" );
                        }
                        if ( LOG.isDebugEnabled() && ( (HibernateProxy) proxy ).getHibernateLazyInitializer().isUnwrap() ) {
                            LOG.debug( "Ignoring NO_PROXY to honor laziness" );
                        }

						return completedFuture( persistenceContext.narrowProxy( proxy, entityDescriptor, entityKey, null ) );
					}

					// specialized handling for entities with subclasses with a HibernateProxy factory
					if ( entityMetamodel.hasSubclasses() ) {
						// entities with subclasses that define a ProxyFactory can create
						// a HibernateProxy.
//                        LOG.debugf( "Creating a HibernateProxy for to-one association with subclasses to honor laziness" );
						return completedFuture( createProxy( entityKey ) );
					}
					return completedFuture( enhancementMetadata.createEnhancedProxy( entityKey, false, this ) );
				}
				else if ( !entityMetamodel.hasSubclasses() ) {
					return completedFuture( enhancementMetadata.createEnhancedProxy( entityKey, false, this ) );
				}
				// If we get here, then the entity class has subclasses and there is no HibernateProxy factory.
				// The entity will get loaded below.
			}
			else {
				if ( entityDescriptor.hasProxy() ) {
					final Object existingProxy = persistenceContext.getProxy( entityKey );
					if ( existingProxy != null ) {
						return completedFuture( persistenceContext.narrowProxy( existingProxy, entityDescriptor, entityKey, null ) );
					}
					else {
						return completedFuture( createProxy( entityKey ) );
					}
				}
			}
		}

		// otherwise immediately materialize it

		// IMPLEMENTATION NOTE: increment/decrement the load count before/after getting the value
		//                      to ensure that #get does not clear the PersistenceContext.
		persistenceContext.beforeLoad();
		return this.<Object>reactiveGet( entityDescriptor.getMappedClass(), id )
				.whenComplete( (r, e) -> persistenceContext.afterLoad() );
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy) {
		checkOpen();
		if ( association == null ) {
			return CompletionStages.nullFuture();
		}

		PersistenceContext persistenceContext = getPersistenceContext();
		if ( association instanceof HibernateProxy ) {
			LazyInitializer initializer = ( (HibernateProxy) association ).getHibernateLazyInitializer();
			if ( initializer.isUninitialized() ) {
				String entityName = initializer.getEntityName();
				Object id = initializer.getIdentifier();
				initializer.setSession( this );
				persistenceContext.beforeLoad();

				final ReactiveEntityPersister entityDescriptor = (ReactiveEntityPersister) getFactory().getRuntimeMetamodels()
						.getMappingMetamodel()
						.getEntityDescriptor( entityName );

				// This is hard to test because it happens on slower machines like the ones we use on CI.
				// See AbstractLazyInitializer#initialize, it happens when the object is not initialized and we need to
				// call session.immediateLoad
				CompletionStage<?> stage = initializer.getImplementation() instanceof CompletionStage
						? (CompletionStage<?>) initializer.getImplementation()
						: completedFuture( initializer.getImplementation() );

				return stage
						.thenCompose( implementation -> entityDescriptor
								.reactiveLoad( id, implementation, LockOptions.NONE, this )
								.whenComplete( (v, e) -> {
									persistenceContext.afterLoad();
									if ( persistenceContext.isLoadFinished() ) {
										persistenceContext.clear();
									}
								} )
								.thenApply( entity -> {
									checkEntityFound( this, entityName, id, entity );
									initializer.setImplementation( entity );
									initializer.unsetSession();
									return unproxy ? (T) entity : association;
								} ) );
			}
			else {
				// Initialized
				return completedFuture( unproxy ? (T) initializer.getImplementation() : association );
			}
		}
		else if ( association instanceof PersistentCollection ) {
			PersistentCollection persistentCollection = (PersistentCollection) association;
			if ( persistentCollection.wasInitialized() ) {
				return completedFuture( association );
			}
			else {
				final ReactiveCollectionPersister collectionDescriptor = (ReactiveCollectionPersister) getFactory()
						.getRuntimeMetamodels()
						.getMappingMetamodel()
						.getCollectionDescriptor( persistentCollection.getRole() );

				Object key = persistentCollection.getKey();
				persistenceContext.addUninitializedCollection( collectionDescriptor, persistentCollection, key );
				persistentCollection.setCurrentSession( this );
				return collectionDescriptor.reactiveInitialize( key, this )
						.whenComplete( (v, e) -> {
							if ( persistenceContext.isLoadFinished() ) {
								persistenceContext.clear();
							}
						} )
						.thenApply( v -> association );
			}
		}
		else if ( isPersistentAttributeInterceptable( association ) ) {
			final PersistentAttributeInterceptable interceptable = asPersistentAttributeInterceptable( association );
			final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
			if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor ) {
				EnhancementAsProxyLazinessInterceptor eapli = (EnhancementAsProxyLazinessInterceptor) interceptor;
				return forceInitialize( association, null, eapli.getIdentifier(), eapli.getEntityName(), this )
						.thenApply( i -> association );

			}
			else {
				return CompletionStages.completedFuture( association );
			}
		}
		else {
			return completedFuture( association );
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity, String name) {
		RootGraphImplementor<?> entityGraph = createEntityGraph( name );
		if ( !entityGraph.getGraphedType().getJavaType().equals( entity ) ) {
			throw LOG.wrongEntityType();
		}
		return (RootGraphImplementor<T>) entityGraph;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> RootGraphImplementor<T> getEntityGraph(Class<T> entity, String name) {
		RootGraphImplementor<?> entityGraph = getEntityGraph( name );
		if ( !entityGraph.getGraphedType().getJavaType().equals( entity ) ) {
			throw LOG.wrongEntityType();
		}
		return (RootGraphImplementor<T>) entityGraph;
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(String queryString) {
		return null;
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(String queryString, Class<R> resultType) {
		return null;
	}

	@Override
	public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity) {
		return new RootGraphImpl<>( null,
				getFactory().getJpaMetamodel().entity( entity ),
				getSessionFactory().getJpaMetamodel() );
	}

	private RootGraphImplementor<?> createEntityGraph(String graphName) {
		checkOpen();
		final RootGraphImplementor<?> named = getFactory().findEntityGraphByName( graphName );
		return named != null
				? named.makeRootGraph( graphName, true )
				: null;
	}

	private RootGraphImplementor<?> getEntityGraph(String graphName) {
		checkOpen();
		final RootGraphImplementor<?> named = getFactory().findEntityGraphByName( graphName );
		if ( named == null ) {
			throw new IllegalArgumentException( "Could not locate EntityGraph with given name : " + graphName );
		}
		return named;
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException( "Non reactive close method called. Use close(CompletableFuture<Void> closing) instead." );
	}

	@Override
	public void close(CompletableFuture<Void> closing) {
		reactiveConnection.close()
				.thenAccept( v -> super.close() )
				.whenComplete( (unused, throwable) -> {
					if ( throwable != null ) {
						closing.completeExceptionally( throwable );
					}
					else {
						closing.complete( null );
					}
				} );
	}
}

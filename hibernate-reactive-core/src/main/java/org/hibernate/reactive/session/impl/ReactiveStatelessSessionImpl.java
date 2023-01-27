/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.UnknownEntityTypeException;
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
import org.hibernate.internal.util.StringHelper;
import org.hibernate.jpa.spi.NativeQueryTupleTransformer;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.query.IllegalMutationQueryException;
import org.hibernate.query.IllegalNamedQueryOptionsException;
import org.hibernate.query.IllegalSelectQueryException;
import org.hibernate.query.QueryTypeMismatchException;
import org.hibernate.query.criteria.JpaCriteriaInsertSelect;
import org.hibernate.query.hql.spi.SqmQueryImplementor;
import org.hibernate.query.named.NamedResultSetMappingMemento;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.query.sql.internal.NativeQueryImpl;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.query.sql.spi.NativeQueryImplementor;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.NamedSqmQueryMemento;
import org.hibernate.query.sqm.tree.SqmDmlStatement;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertSelectStatement;
import org.hibernate.query.sqm.tree.select.SqmQueryGroup;
import org.hibernate.query.sqm.tree.select.SqmQuerySpec;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.query.ReactiveMutationQuery;
import org.hibernate.reactive.query.ReactiveNativeQuery;
import org.hibernate.reactive.query.ReactiveQuery;
import org.hibernate.reactive.query.ReactiveQueryImplementor;
import org.hibernate.reactive.query.ReactiveSelectionQuery;
import org.hibernate.reactive.query.sql.internal.ReactiveNativeQueryImpl;
import org.hibernate.reactive.query.sql.spi.ReactiveNativeQueryImplementor;
import org.hibernate.reactive.query.sqm.iternal.ReactiveQuerySqmImpl;
import org.hibernate.reactive.query.sqm.iternal.ReactiveSqmSelectionQueryImpl;
import org.hibernate.reactive.session.ReactiveSqmQueryImplementor;
import org.hibernate.reactive.session.ReactiveStatelessSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.spi.TypeConfiguration;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.Tuple;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;

import static java.lang.Boolean.TRUE;
import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.internal.util.StringHelper.isEmpty;
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
public class ReactiveStatelessSessionImpl extends StatelessSessionImpl implements ReactiveStatelessSession {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveConnection reactiveConnection;

	private final ReactiveStatelessSession batchingHelperSession;

	private final PersistenceContext persistenceContext;

	public ReactiveStatelessSessionImpl(SessionFactoryImpl factory, SessionCreationOptions options, ReactiveConnection connection) {
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
		//The checkOpen check is invoked on all most used public API, making it an
		//excellent hook to also check for the right thread to be used
		//(which is an assertion so costs us nothing in terms of performance, after inlining).
		threadCheck();
		super.checkOpen();
	}

	private void threadCheck() {
		// FIXME: We should check the threads like we do in ReactiveSessionImpl
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
						boolean substitute = Versioning
								.seedVersion( state, persister.getVersionProperty(), persister.getVersionMapping(), this );
						if ( substitute ) {
							persister.setValues( entity, state );
						}
					}

					if ( persister.isIdentifierAssignedByInsert() ) {
						return persister.insertReactive( state, entity, this )
								.thenAccept( generatedId -> assignIdIfNecessary( entity, generatedId, persister, this ) );
					}
					id = assignIdIfNecessary( id, entity, persister, this );
					persister.setIdentifier( entity, id, this );
					return persister.insertReactive( id, state, entity, this );
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
	public <R> ReactiveSqmQueryImplementor<R> createReactiveQuery(String queryString) {
		return createReactiveQuery( queryString, null );
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(CriteriaQuery<R> criteriaQuery) {
		checkOpen();

		try {
			final SqmSelectStatement<R> selectStatement = (SqmSelectStatement<R>) criteriaQuery;
			if ( ! ( selectStatement.getQueryPart() instanceof SqmQueryGroup ) ) {
				final SqmQuerySpec<R> querySpec = selectStatement.getQuerySpec();
				if ( querySpec.getSelectClause().getSelections().isEmpty() ) {
					if ( querySpec.getFromClause().getRoots().size() == 1 ) {
						querySpec.getSelectClause().setSelection( querySpec.getFromClause().getRoots().get(0) );
					}
				}
			}

			return new ReactiveQuerySqmImpl<R>( selectStatement, criteriaQuery.getResultType(), this );
		}
		catch (RuntimeException e) {
			if ( getSessionFactory().getJpaMetamodel().getJpaCompliance().isJpaTransactionComplianceEnabled() ) {
				markForRollbackOnly();
			}
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public void prepareForQueryExecution(boolean requiresTxn) {
		checkOpen();
		checkTransactionSynchStatus();

		// FIXME: this does not work at the moment
//		if ( requiresTxn && !isTransactionInProgress() ) {
//			throw new TransactionRequiredException(
//					"Query requires transaction be in progress, but no transaction is known to be in progress"
//			);
//		}
	}

	@Override
	public <R> ReactiveSqmQueryImplementor<R> createReactiveQuery(String queryString, Class<R> expectedResultType) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			final QueryEngine queryEngine = getFactory().getQueryEngine();
			final QueryInterpretationCache interpretationCache = queryEngine.getInterpretationCache();

			final ReactiveQuerySqmImpl<R> query = new ReactiveQuerySqmImpl<R>(
					queryString,
					interpretationCache.resolveHqlInterpretation(
							queryString,
							expectedResultType,
							s -> queryEngine.getHqlTranslator().translate( queryString, expectedResultType )
					),
					expectedResultType,
					this
			);

			applyQuerySettingsAndHints( query );
			query.setComment( queryString );

			return query;
		}
		catch (RuntimeException e) {
			markForRollbackOnly();
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveNativeQueryImplementor<R> createReactiveNativeQuery(String sqlString) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			ReactiveNativeQueryImpl query = new ReactiveNativeQueryImpl( sqlString, this);

			if ( StringHelper.isEmpty( query.getComment() ) ) {
				query.setComment( "dynamic native SQL query" );
			}
			applyQuerySettingsAndHints( query );
			return query;
		}
		catch (RuntimeException he) {
			throw getExceptionConverter().convert( he );
		}
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, Class<R> resultClass) {
		ReactiveNativeQuery query = createReactiveNativeQuery( sqlString );
		if ( Tuple.class.equals( resultClass ) ) {
			query.setTupleTransformer( new NativeQueryTupleTransformer() );
		}
		else if ( getFactory().getMappingMetamodel().isEntityClass( resultClass ) ) {
			query.addEntity( "alias1", resultClass.getName(), LockMode.READ );
		}
		else {
			( (NativeQueryImpl) query ).addScalar( 1, resultClass );
		}
		return query;
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(
			String sqlString,
			Class<R> resultClass,
			String tableAlias) {
		ReactiveNativeQuery<R> query = createReactiveNativeQuery( sqlString );
		if ( getFactory().getMappingMetamodel().isEntityClass(resultClass) ) {
			query.addEntity( tableAlias, resultClass.getName(), LockMode.READ );
			return query;
		}

		throw new UnknownEntityTypeException( "unable to locate persister: " + resultClass.getName() );
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, String resultSetMappingName) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			if ( StringHelper.isNotEmpty( resultSetMappingName ) ) {
				final NamedResultSetMappingMemento resultSetMappingMemento = getFactory().getQueryEngine()
						.getNamedObjectRepository()
						.getResultSetMappingMemento( resultSetMappingName );

				if ( resultSetMappingMemento == null ) {
					throw new HibernateException( "Could not resolve specified result-set mapping name : " + resultSetMappingName );
				}
				return new ReactiveNativeQueryImpl<>( sqlString, resultSetMappingMemento, this );
			}
			else {
				return new ReactiveNativeQueryImpl<>( sqlString, this );
			}
			//TODO: why no applyQuerySettingsAndHints( query ); ???
		}
		catch (RuntimeException he) {
			throw getExceptionConverter().convert( he );
		}
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(
			String sqlString,
			String resultSetMappingName,
			Class<R> resultClass) {
		final ReactiveNativeQuery<R> query = createReactiveNativeQuery( sqlString, resultSetMappingName );
		if ( Tuple.class.equals( resultClass ) ) {
			query.setTupleTransformer( new NativeQueryTupleTransformer() );
		}
		return query;
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString) {
		return internalCreateSelectionQuery( hqlString, null );
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString, Class<R> resultType) {
		return internalCreateSelectionQuery( hqlString, resultType );
	}


	private <R> ReactiveSelectionQuery<R> internalCreateSelectionQuery(String hqlString, Class<R> expectedResultType) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			final QueryEngine queryEngine = getFactory().getQueryEngine();
			final QueryInterpretationCache interpretationCache = queryEngine.getInterpretationCache();
			final HqlInterpretation hqlInterpretation = interpretationCache.resolveHqlInterpretation(
					hqlString,
					expectedResultType,
					(s) -> queryEngine.getHqlTranslator().translate( hqlString, expectedResultType )
			);

			if ( !( hqlInterpretation.getSqmStatement() instanceof SqmSelectStatement ) ) {
				throw new IllegalSelectQueryException( "Expecting a selection query, but found `" + hqlString + "`", hqlString );
			}

			final ReactiveSqmSelectionQueryImpl<R> query = new ReactiveSqmSelectionQueryImpl<>(
					hqlString,
					hqlInterpretation,
					expectedResultType,
					this
			);

			if ( expectedResultType != null ) {
				final Class<?> resultType = query.getResultType();
				if ( ! expectedResultType.isAssignableFrom( resultType ) ) {
					throw new QueryTypeMismatchException(
							String.format(
									Locale.ROOT,
									"Query result-type error - expecting `%s`, but found `%s`",
									expectedResultType.getName(),
									resultType.getName()
							)
					);
				}
			}

			query.setComment( hqlString );
			applyQuerySettingsAndHints( query );

			//noinspection unchecked
			return query;
		}
		catch (RuntimeException e) {
			markForRollbackOnly();
			throw e;
		}
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(CriteriaQuery<R> criteria) {
		SqmUtil.verifyIsSelectStatement( (SqmStatement<R>) criteria, null );
		return new ReactiveSqmSelectionQueryImpl<R>( (SqmSelectStatement<R>) criteria, criteria.getResultType(), this );
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(String hqlString) {
		final QueryImplementor<?> query = createQuery( hqlString );
		final SqmStatement<R> sqmStatement = ( (SqmQueryImplementor<R>) query ).getSqmStatement();
		checkMutationQuery( hqlString, sqmStatement );
		return new ReactiveQuerySqmImpl<R>( sqmStatement, null, this );
	}

	// Change visibility in ORM
	private static void checkMutationQuery(String hqlString, SqmStatement<?> sqmStatement) {
		if ( !( sqmStatement instanceof SqmDmlStatement ) ) {
			throw new IllegalMutationQueryException( "Expecting a mutation query, but found `" + hqlString + "`" );
		}
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaUpdate updateQuery) {
		checkOpen();
		try {
			return new ReactiveQuerySqmImpl<R>( (SqmUpdateStatement<R>) updateQuery, null, this );
		}
		catch ( RuntimeException e ) {
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaDelete deleteQuery) {
		checkOpen();
		try {
			return new ReactiveQuerySqmImpl<R>( (SqmDeleteStatement<R>) deleteQuery, null, this );
		}
		catch ( RuntimeException e ) {
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(JpaCriteriaInsertSelect insertSelect) {
		checkOpen();
		try {
			return new ReactiveQuerySqmImpl<>( (SqmInsertSelectStatement<R>) insertSelect, null, this );
		}
		catch ( RuntimeException e ) {
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String queryName) {
		return (ReactiveSelectionQuery<R>) createNamedSelectionQuery( queryName, null );
	}

	@Override
	public <R> ReactiveMutationQuery<R> createNamedReactiveMutationQuery(String queryName) {
		return (ReactiveMutationQuery<R>) buildNamedQuery(
				queryName,
				memento -> createSqmQueryImplementor( queryName, memento ),
				memento -> createNativeQueryImplementor( queryName, memento )
		);
	}

	// Copy and paste from ORM: change the visibility instead
	private NativeQueryImplementor<?> createNativeQueryImplementor(String queryName, NamedNativeQueryMemento memento) {
		final NativeQueryImplementor<?> query = memento.toQuery( this );
		final Boolean isUnequivocallySelect = query.isSelectQuery();
		if ( isUnequivocallySelect == TRUE ) {
			throw new IllegalMutationQueryException(
					"Expecting named native query (" + queryName + ") to be a mutation query, but found `"
							+ memento.getSqlString() + "`"
			);
		}
		if ( isEmpty( query.getComment() ) ) {
			query.setComment( "dynamic native-SQL query" );
		}
		applyQuerySettingsAndHints( query );
		return query;
	}

	// Copy and paste from ORM: change the visibility instead
	private SqmQueryImplementor<?> createSqmQueryImplementor(String queryName, NamedSqmQueryMemento memento) {
		final SqmQueryImplementor<?> query = memento.toQuery( this );
		final SqmStatement<?> sqmStatement = query.getSqmStatement();
		if ( !( sqmStatement instanceof SqmDmlStatement ) ) {
			throw new IllegalMutationQueryException(
					"Expecting a named mutation query (" + queryName + "), but found a select statement"
			);
		}
		if ( memento.getLockOptions() != null && ! memento.getLockOptions().isEmpty() ) {
			throw new IllegalNamedQueryOptionsException(
					"Named mutation query `" + queryName + "` specified lock-options"
			);
		}
		if ( isEmpty( query.getComment() ) ) {
			query.setComment( "dynamic HQL query" );
		}
		applyQuerySettingsAndHints( query );
		return query;
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String queryName, Class<R> expectedResultType) {
		return (ReactiveSelectionQuery<R>) createNamedSelectionQuery( queryName , expectedResultType );
	}

	@Override
	public <R> ReactiveMutationQuery<R> createNativeReactiveMutationQuery(String sqlString) {
		final ReactiveNativeQueryImplementor<R> query = createReactiveNativeQuery( sqlString );
		if ( query.isSelectQuery() == TRUE ) {
			throw new IllegalMutationQueryException( "Expecting a native mutation query, but found `" + sqlString + "`" );
		}
		return query;
	}

	@Override
	public <R> ReactiveQueryImplementor<R> createReactiveNamedQuery(String queryName, Class<R> resultType) {
		return (ReactiveQueryImplementor<R>) buildNamedQuery( queryName, resultType );
	}

	@Override
	public <R> ReactiveQuery getNamedReactiveQuery(String queryName) {
		return (ReactiveQueryImplementor) buildNamedQuery( queryName, null );
	}

	@Override
	public <R> ReactiveNativeQuery getNamedReactiveNativeQuery(String name) {
		return (ReactiveNativeQuery) getNamedNativeQuery( name );
	}

	@Override
	public ReactiveNativeQuery getNamedReactiveNativeQuery(String name, String resultSetMapping) {
		return (ReactiveNativeQuery) getNamedNativeQuery( name, resultSetMapping );
	}

	@Override
	public <R> ReactiveNativeQuery createReactiveNativeQuery(String queryString, AffectedEntities affectedEntities) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			ReactiveNativeQueryImpl query = new ReactiveNativeQueryImpl( queryString, this );
			String[] spaces = affectedEntities.getAffectedSpaces( getFactory() );
			for ( String space : spaces ) {
				query.addSynchronizedQuerySpace( space );
			}
			if ( StringHelper.isEmpty( query.getComment() ) ) {
				query.setComment( "dynamic native SQL query" );
			}
			applyQuerySettingsAndHints( query );
			return query;
		}
		catch (RuntimeException he) {
			throw getExceptionConverter().convert( he );
		}
	}

	@Override
	public <R> ReactiveNativeQuery createReactiveNativeQuery(
			String queryString,
			Class<R> resultType,
			AffectedEntities affectedEntities) {
		ReactiveNativeQuery query = createReactiveNativeQuery( queryString, affectedEntities );
		return addResultType( resultType, query );
	}

	private <T> ReactiveNativeQuery addResultType(Class<T> resultClass, ReactiveNativeQuery query) {
		if ( Tuple.class.equals( resultClass ) ) {
			query.setTupleTransformer( new NativeQueryTupleTransformer() );
		}
		else if ( getFactory().getMappingMetamodel().isEntityClass( resultClass ) ) {
			query.addEntity( "alias1", resultClass.getName(), LockMode.READ );
		}
		else if ( resultClass != Object.class && resultClass != Object[].class ) {
			query.addScalar( 1, resultClass );
		}
		return query;
	}

	@Override
	public <R> ReactiveNativeQuery createReactiveNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
		throw LOG.notYetImplemented();
	}

	@Override
	public <R> ReactiveNativeQuery createReactiveNativeQuery(
			String queryString,
			ResultSetMapping<R> resultSetMapping,
			AffectedEntities affectedEntities) {
		throw LOG.notYetImplemented();
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

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		throw LOG.notYetImplemented();
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
		throw LOG.nonReactiveMethodCall( "close(CompletableFuture<Void> closing)" );
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

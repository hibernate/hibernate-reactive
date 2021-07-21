/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.UnresolvableObjectException;
import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.query.spi.sql.NativeSQLQuerySpecification;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.NamedQueryDefinition;
import org.hibernate.engine.spi.NamedSQLQueryDefinition;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.internal.RootGraphImpl;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.StatelessSessionImpl;
import org.hibernate.jpa.spi.CriteriaQueryTupleTransformer;
import org.hibernate.jpa.spi.NativeQueryTupleTransformer;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.loader.custom.sql.SQLCustomQuery;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.loader.custom.impl.ReactiveCustomLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.collection.impl.ReactiveCollectionPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.CriteriaQueryOptions;
import org.hibernate.reactive.session.ReactiveNativeQuery;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveStatelessSession;
import org.hibernate.tuple.entity.EntityMetamodel;

import javax.persistence.EntityGraph;
import javax.persistence.Tuple;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.id.impl.IdentifierGeneration.assignIdIfNecessary;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.generateId;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
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
    private final boolean allowBytecodeProxy;

    private final ReactiveStatelessSession batchingHelperSession;

    private final PersistenceContext persistenceContext;

    public ReactiveStatelessSessionImpl(SessionFactoryImpl factory,
                                        SessionCreationOptions options,
                                        ReactiveConnection connection) {
        super(factory, options);
        reactiveConnection = connection;
        allowBytecodeProxy = getFactory().getSessionFactoryOptions().isEnhancementAsProxyEnabled();
        persistenceContext = new ReactivePersistenceContextAdapter(this);
        batchingHelperSession = new ReactiveStatelessSessionImpl(factory, options, connection, persistenceContext);
    }

    /**
     * Create a helper instance with an underling {@link BatchingConnection}
     */
    private ReactiveStatelessSessionImpl(SessionFactoryImpl factory,
                                         SessionCreationOptions options,
                                         ReactiveConnection connection,
                                         PersistenceContext persistenceContext) {
        super(factory, options);
        Integer batchSize = getConfiguredJdbcBatchSize();
        reactiveConnection = batchSize==null || batchSize<2 ? connection :
                new BatchingConnection( connection, batchSize );
        allowBytecodeProxy = getFactory().getSessionFactoryOptions().isEnhancementAsProxyEnabled();
        this.persistenceContext = persistenceContext;
        batchingHelperSession = this;
    }

    private LockOptions getNullSafeLockOptions(LockMode lockMode) {
        return new LockOptions( lockMode == null ? LockMode.NONE : lockMode );
    }

    @Override
    public PersistenceContext getPersistenceContext() {
        return persistenceContext;
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
        return reactiveGet( entityClass, id, LockMode.NONE, null );
    }

    @Override
    public <T> CompletionStage<T> reactiveGet(Class<? extends T> entityClass, Object id, LockMode lockMode,
                                              EntityGraph<T> fetchGraph) {
        checkOpen();

        if ( fetchGraph!=null ) {
            getLoadQueryInfluencers()
                    .getEffectiveEntityGraph()
                    .applyGraph( (RootGraphImplementor<T>) fetchGraph, GraphSemantic.FETCH );
        }

        ReactiveEntityPersister persister = (ReactiveEntityPersister)
                getFactory().getMetamodel().entityPersister(entityClass);
        LockOptions lockOptions = getNullSafeLockOptions(lockMode);
        return persister.reactiveLoad( (Serializable) id, null, lockOptions, this )
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
        return (ReactiveEntityPersister) super.getEntityPersister(entityName, object);
    }

    @Override
    public CompletionStage<Void> reactiveInsert(Object entity) {
        checkOpen();
        ReactiveEntityPersister persister = getEntityPersister( null, entity );
        return generateId( entity, persister, this, this )
                .thenCompose( id -> {
                    Object[] state = persister.getPropertyValues(entity);
                    if ( persister.isVersioned() ) {
                        boolean substitute = Versioning.seedVersion(
                                state,
                                persister.getVersionProperty(),
                                persister.getVersionType(),
                                this
                        );
                        if (substitute) {
                            persister.setPropertyValues( entity, state );
                        }
                    }

                    if ( persister.isIdentifierAssignedByInsert() ) {
                        return persister.insertReactive( state, entity, this )
                                .thenAccept( generatedId -> assignIdIfNecessary( entity, generatedId, persister,this ) );
                    }
                    else {
                        id = assignIdIfNecessary( id, entity, persister,this );
                        persister.setIdentifier( entity, id, this );
                        return persister.insertReactive( id, state, entity, this );
                    }
                } );
    }

    @Override
    public CompletionStage<Void> reactiveDelete(Object entity) {
        checkOpen();
        ReactiveEntityPersister persister = getEntityPersister( null, entity );
        Serializable id = persister.getIdentifier( entity, this );
        Object version = persister.getVersion( entity );
        return persister.deleteReactive( id, version, entity, this );
    }

    @Override
    public CompletionStage<Void> reactiveUpdate(Object entity) {
        checkOpen();
        if ( entity instanceof HibernateProxy ) {
            final LazyInitializer hibernateLazyInitializer = ( (HibernateProxy) entity ).getHibernateLazyInitializer();
            if ( hibernateLazyInitializer.isUninitialized() ) {
                return reactiveFetch( entity, true ).thenCompose(
                        fetchedEntity -> executeReactiveUpdate( fetchedEntity ) );
            }
            else {
                return executeReactiveUpdate( hibernateLazyInitializer.getImplementation() );
            }
        }

        return executeReactiveUpdate( entity );
    }

    private CompletionStage<Void> executeReactiveUpdate(Object entity) {
        ReactiveEntityPersister persister = getEntityPersister( null, entity );
        Serializable id = persister.getIdentifier( entity, this );
        Object[] state = persister.getPropertyValues( entity );
        Object oldVersion;
        if ( persister.isVersioned() ) {
            oldVersion = persister.getVersion( entity );
            Object newVersion = Versioning.increment( oldVersion, persister.getVersionType(), this );
            Versioning.setVersion( state, newVersion, persister );
            persister.setPropertyValues( entity, state );
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
        final Serializable id = persister.getIdentifier( entity, this );

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
                .whenComplete( (v,e) -> getLoadQueryInfluencers().setInternalFetchProfile( previousFetchProfile ) );
    }

    @Override
    public CompletionStage<Void> reactiveInsertAll(Object... entities) {
        return loop(entities, batchingHelperSession::reactiveInsert)
                .thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
    }

    @Override
    public CompletionStage<Void> reactiveUpdateAll(Object... entities) {
        return loop(entities, batchingHelperSession::reactiveUpdate)
                .thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
    }

    @Override
    public CompletionStage<Void> reactiveDeleteAll(Object... entities) {
        return loop(entities, batchingHelperSession::reactiveDelete)
                .thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
    }

    @Override
    public CompletionStage<Void> reactiveRefreshAll(Object... entities) {
        return loop(entities, batchingHelperSession::reactiveRefresh)
                .thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
    }

    @Override
    public <R> ReactiveQueryImpl<R> createReactiveQuery(String queryString) {
        checkOpen();

        try {
            ReactiveQueryImpl<R> query =
                    new ReactiveQueryImpl<>( this, getQueryPlan( queryString, false ), queryString );
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
    public <R> ReactiveQuery<R> createReactiveQuery(String queryString, Class<R> resultType) {
        try {
            // do the translation
            final ReactiveQueryImpl<R> query = createReactiveQuery( queryString );
            resultClassChecking( resultType, query );
            return query;
        }
        catch (RuntimeException e) {
            throw getExceptionConverter().convert( e );
        }
    }

    private <T> void handleNativeQueryResult(ReactiveNativeQuery<T> query, Class<T> resultClass) {
        if ( Tuple.class.equals( resultClass ) ) {
            query.setResultTransformer( new NativeQueryTupleTransformer() );
        }
        else {
            query.addEntity( "alias1", resultClass.getName(), LockMode.READ );
        }
    }

    @Override
    public <R> ReactiveQuery<R> createReactiveNamedQuery(String name) {
        return buildReactiveQueryFromName( name, null );
    }

    @Override
    public <R> ReactiveQuery<R> createReactiveNamedQuery(String name, Class<R> resultClass) {
        return buildReactiveQueryFromName( name, resultClass );
    }

    //TODO nasty copy/paste
    private <T> ReactiveQuery<T> buildReactiveQueryFromName(String name, Class<T> resultType) {
        checkOpen();
        try {
            pulseTransactionCoordinator();
            delayedAfterCompletion();

            // todo : apply stored setting at the JPA Query level too

            NamedQueryDefinition namedQueryDefinition =
                    getFactory().getNamedQueryRepository()
                            .getNamedQueryDefinition( name );
            if ( namedQueryDefinition != null ) {
                return createReactiveQuery( namedQueryDefinition, resultType );
            }

            NamedSQLQueryDefinition nativeQueryDefinition =
                    getFactory().getNamedQueryRepository()
                            .getNamedSQLQueryDefinition( name );
            if ( nativeQueryDefinition != null ) {
                return createReactiveNativeQuery( nativeQueryDefinition, resultType );
            }

            throw getExceptionConverter().convert(
                    new IllegalArgumentException( "no query defined for name '" + name + "'" )
            );
        }
        catch (RuntimeException e) {
            throw !( e instanceof IllegalArgumentException ) ? new IllegalArgumentException( e ) : e;
        }
    }

    //TODO fix nasty copy/paste
    private <T> ReactiveQuery<T> createReactiveQuery(NamedQueryDefinition namedQueryDefinition,
                                                     Class<T> resultType) {
        final ReactiveQuery<T> query = createReactiveQuery( namedQueryDefinition );
        if ( resultType != null ) {
            resultClassChecking( resultType, createQuery( namedQueryDefinition ) );
        }
        return query;
    }

    //TODO fix nasty copy/paste
    private <T> ReactiveQuery<T> createReactiveQuery(NamedQueryDefinition queryDefinition) {
        String queryString = queryDefinition.getQueryString();
        ReactiveQueryImpl<T> query = new ReactiveQueryImpl<>( this, getQueryPlan( queryString, false ), queryString );
        applyQuerySettingsAndHints( query );
        query.setHibernateFlushMode( queryDefinition.getFlushMode() );
        query.setComment( comment( queryDefinition ) );
        if ( queryDefinition.getLockOptions() != null ) {
            query.setLockOptions( queryDefinition.getLockOptions() );
        }

        initQueryFromNamedDefinition( query, queryDefinition );

        return query;
    }

    //TODO fix nasty copy/paste
    private <T> ReactiveNativeQuery<T> createReactiveNativeQuery(NamedSQLQueryDefinition queryDefinition,
                                                                 Class<T> resultType) {
        if ( resultType != null
                && !Tuple.class.equals( resultType )
                && !Object[].class.equals( resultType ) ) {
            resultClassChecking( resultType, queryDefinition );
        }

        ReactiveNativeQueryImpl<T> query = new ReactiveNativeQueryImpl<>(
                queryDefinition,
                this,
                getFactory().getQueryPlanCache()
                        .getSQLParameterMetadata( queryDefinition.getQueryString(), false )
        );
        if ( Tuple.class.equals( resultType ) ) {
            query.setResultTransformer( new NativeQueryTupleTransformer() );
        }
        applyQuerySettingsAndHints( query );
        query.setHibernateFlushMode( queryDefinition.getFlushMode() );
        query.setComment( comment(queryDefinition) );
        if ( queryDefinition.getLockOptions() != null ) {
            query.setLockOptions( queryDefinition.getLockOptions() );
        }

        initQueryFromNamedDefinition( query, queryDefinition );

        return query;
    }

    @Override
    public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString) {
        checkOpen();

        try {
            ParameterMetadata params = getFactory().getQueryPlanCache()
                    .getSQLParameterMetadata(sqlString, false);
            ReactiveNativeQueryImpl<T> query =
                    new ReactiveNativeQueryImpl<>(sqlString, false, this, params );
            query.setComment( "dynamic native SQL query" );
            applyQuerySettingsAndHints( query );
            return query;
        }
        catch ( RuntimeException he ) {
            throw getExceptionConverter().convert( he );
        }
    }

    @Override
    public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultClass) {
        try {
            ReactiveNativeQuery<T> query = createReactiveNativeQuery( sqlString );
            handleNativeQueryResult( query, resultClass );
            return query;
        }
        catch ( RuntimeException he ) {
            throw getExceptionConverter().convert( he );
        }
    }

    @Override
    public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, String resultSetMapping) {
        try {
            ReactiveNativeQuery<T> query = createReactiveNativeQuery( sqlString );
            query.setResultSetMapping( resultSetMapping );
            return query;
        }
        catch ( RuntimeException he ) {
            throw getExceptionConverter().convert( he );
        }
    }

    @SuppressWarnings("unchecked")
    private <T> ReactiveHQLQueryPlan<T> getReactivePlan(String query, QueryParameters parameters) {
        HQLQueryPlan plan = parameters.getQueryPlan();
        if (plan == null) {
            plan = getQueryPlan( query, false );
        }
        return (ReactiveHQLQueryPlan<T>) plan;
    }

    @Override
    public <T> CompletionStage<List<T>> reactiveList(String query, QueryParameters parameters) {
        checkOpen();
        parameters.validateParameters();

        ReactiveHQLQueryPlan<T> reactivePlan = getReactivePlan( query, parameters );
        return reactivePlan.performReactiveList( parameters, this )
                .whenComplete( (list, x) -> {
                    getPersistenceContext().clear();
                    afterOperation( x == null );
                } );
    }

    @Override
    public <T> CompletionStage<List<T>> reactiveList(NativeSQLQuerySpecification spec, QueryParameters parameters) {
        checkOpen();

        ReactiveCustomLoader<T> loader = new ReactiveCustomLoader<>(
                getNativeQueryPlan( spec ).getCustomQuery(),
                getFactory()
        );

        return loader.reactiveList(this, parameters)
                .whenComplete( (r, x) -> {
                    getPersistenceContext().clear();
                    afterOperation(x == null);
                } );
    }

    private static String comment(NamedQueryDefinition queryDefinition) {
        return queryDefinition.getComment() != null
                ? queryDefinition.getComment()
                : queryDefinition.getName();
    }

    @SuppressWarnings("unchecked")
    private ReactiveHQLQueryPlan<Void> getReactivePlan(String query) {
        return (ReactiveHQLQueryPlan<Void>) getQueryPlan( query, false );
    }

    @Override
    public CompletionStage<Integer> executeReactiveUpdate(String query, QueryParameters parameters) {
        checkOpen();
        parameters.validateParameters();
        return getReactivePlan( query )
                .performExecuteReactiveUpdate( parameters, this )
                .whenComplete( (count, x) -> {
                    getPersistenceContext().clear();
                    afterOperation( x == null );
                } );
    }

    @Override
    public CompletionStage<Integer> executeReactiveUpdate(NativeSQLQuerySpecification specification,
                                                          QueryParameters parameters) {
        checkOpen();
        parameters.validateParameters();

        ReactiveNativeSQLQueryPlan reactivePlan = //getNativeQueryPlan( specification );
                new ReactiveNativeSQLQueryPlan(
                        specification.getQueryString(),
                        new SQLCustomQuery(
                                specification.getQueryString(),
                                specification.getQueryReturns(),
                                specification.getQuerySpaces(),
                                getFactory()
                        ) );
        return  reactivePlan.performExecuteReactiveUpdate( parameters, this )
                .whenComplete( (count, x) -> {
                    getPersistenceContext().clear();
                    afterOperation( x == null );
                } );
    }

    @Override
    public void addBulkCleanupAction(BulkOperationCleanupAction action) {
        action.getAfterTransactionCompletionProcess()
                .doAfterTransactionCompletion( true, this );
    }

    @Override
    public List<?> list(String query, QueryParameters queryParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<?> listCustomQuery(CustomQuery customQuery, QueryParameters queryParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
        return ResultSetMappings.resultSetMapping( resultType, mappingName, getFactory() );
    }

    private Object createProxy(EntityKey entityKey) {
        final Object proxy = entityKey.getPersister().createProxy( entityKey.getIdentifier(), this );
        getPersistenceContext().addProxy( entityKey, proxy );
        return proxy;
    }

    @Override
    public CompletionStage<Object> reactiveInternalLoad(String entityName, Serializable id, boolean eager, boolean nullable) {
        checkOpen();

        EntityPersister persister = getFactory().getMetamodel().entityPersister( entityName );
        EntityKey entityKey = generateEntityKey( id, persister );

        // first, try to load it from the temp PC associated to this SS
        PersistenceContext persistenceContext = getPersistenceContext();
        Object loaded = persistenceContext.getEntity( entityKey );
        if ( loaded != null ) {
            // we found it in the temp PC.  Should indicate we are in the midst of processing a result set
            // containing eager fetches via join fetch
            return completedFuture(loaded);
        }

        if ( !eager ) {
            // caller did not request forceful eager loading, see if we can create
            // some form of proxy

            // first, check to see if we can use "bytecode proxies"

            EntityMetamodel entityMetamodel = persister.getEntityMetamodel();
            BytecodeEnhancementMetadata enhancementMetadata = entityMetamodel.getBytecodeEnhancementMetadata();
            if ( allowBytecodeProxy && enhancementMetadata.isEnhancedForLazyLoading() ) {

                // if the entity defines a HibernateProxy factory, see if there is an
                // existing proxy associated with the PC - and if so, use it
                if ( entityMetamodel.getTuplizer().getProxyFactory() != null ) {
                    final Object proxy = persistenceContext.getProxy( entityKey );

                    if ( proxy != null ) {
//                        if ( LOG.isTraceEnabled() ) {
//                            LOG.trace( "Entity proxy found in session cache" );
//                        }
//                        if ( LOG.isDebugEnabled() && ( (HibernateProxy) proxy ).getHibernateLazyInitializer().isUnwrap() ) {
//                            LOG.debug( "Ignoring NO_PROXY to honor laziness" );
//                        }

                        return completedFuture( persistenceContext.narrowProxy( proxy, persister, entityKey, null ) );
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
                if ( persister.hasProxy() ) {
                    final Object existingProxy = persistenceContext.getProxy( entityKey );
                    if ( existingProxy != null ) {
                        return completedFuture( persistenceContext.narrowProxy( existingProxy, persister, entityKey, null ) );
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
        return this.<Object>reactiveGet( persister.getMappedClass(), id )
                .whenComplete( (r, e) -> persistenceContext.afterLoad()  );
    }

    @Override @SuppressWarnings("unchecked")
    public <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy) {
        checkOpen();
        PersistenceContext persistenceContext = getPersistenceContext();
        if ( association instanceof HibernateProxy) {
            LazyInitializer initializer = ((HibernateProxy) association).getHibernateLazyInitializer();
            if ( !initializer.isUninitialized() ) {
                return completedFuture( unproxy ? (T) initializer.getImplementation() : association );
            }
            else {
                String entityName = initializer.getEntityName();
                Serializable id = initializer.getIdentifier();
                ReactiveEntityPersister persister = (ReactiveEntityPersister)
                        getFactory().getMetamodel().entityPersister(entityName);
                initializer.setSession(this);
                persistenceContext.beforeLoad();
                return persister.reactiveLoad( id, initializer.getImplementation(), LockOptions.NONE, this )
                        .whenComplete( (v,e) -> {
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
                        } );
            }
        }
        else if ( association instanceof PersistentCollection) {
            PersistentCollection persistentCollection = (PersistentCollection) association;
            if ( persistentCollection.wasInitialized() ) {
                return completedFuture( association );
            }
            else {
                ReactiveCollectionPersister persister = (ReactiveCollectionPersister)
                        getFactory().getMetamodel().collectionPersister( persistentCollection.getRole() );
                Serializable key = persistentCollection.getKey();
                persistenceContext.addUninitializedCollection( persister, persistentCollection, key );
                persistentCollection.setCurrentSession(this);
                return persister.reactiveInitialize( key, this )
                        .whenComplete( (v,e) -> {
                            if ( persistenceContext.isLoadFinished() ) {
                                persistenceContext.clear();
                            }
                        } )
                        .thenApply( v -> association );
            }
        }
        else {
            return completedFuture( association );
        }
    }

    @Override @SuppressWarnings("unchecked")
    public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity, String name) {
        RootGraphImplementor<?> entityGraph = createEntityGraph(name);
        if ( !entityGraph.getGraphedType().getJavaType().equals(entity) ) {
            throw LOG.wrongEntityType();
        }
        return (RootGraphImplementor<T>) entityGraph;
    }

    @Override @SuppressWarnings("unchecked")
    public <T> RootGraphImplementor<T> getEntityGraph(Class<T> entity, String name) {
        RootGraphImplementor<?> entityGraph = getEntityGraph(name);
        if ( !entityGraph.getGraphedType().getJavaType().equals(entity) ) {
            throw LOG.wrongEntityType();
        }
        return (RootGraphImplementor<T>) entityGraph;
    }

    @Override
    public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity) {
        return new RootGraphImpl<T>( null, getFactory().getMetamodel().entity( entity ), getFactory() );
    }

    private RootGraphImplementor<?> createEntityGraph(String graphName) {
        checkOpen();
        final RootGraphImplementor<?> named = getFactory().findEntityGraphByName( graphName );
        if ( named != null ) {
            return named.makeRootGraph( graphName, true );
        }
        return named;
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
    public <R> ReactiveQuery<R> createReactiveQuery(Criteria<R> criteria) {
        try {
            criteria.validate();
        }
        catch (IllegalStateException ise) {
            throw new IllegalArgumentException( "Error occurred validating the Criteria", ise );
        }

        return criteria.build( newRenderingContext(), this );
    }

    private CriteriaQueryRenderingContext newRenderingContext() {
        return new CriteriaQueryRenderingContext( getFactory() );
    }

    @Override
    public <T> ReactiveQuery<T> createReactiveCriteriaQuery(String jpaqlString,
                                                            Class<T> resultClass,
                                                            CriteriaQueryOptions queryOptions) {
        try {
            ReactiveQuery<T> query = createReactiveQuery( jpaqlString );
            query.setParameterMetadata( queryOptions.getParameterMetadata() );

            boolean hasValueHandlers = queryOptions.getValueHandlers() != null;
            boolean hasTupleElements = Tuple.class.equals( resultClass );

            if ( !hasValueHandlers ) {
                queryOptions.validate( query.getReturnTypes() );
            }

            // determine if we need a result transformer
            if ( hasValueHandlers || hasTupleElements ) {
                query.setResultTransformer( new CriteriaQueryTupleTransformer(
                        queryOptions.getValueHandlers(),
                        hasTupleElements ? queryOptions.getSelection().getCompoundSelectionItems() : null
                ) );
            }

            return query;
        }
        catch ( RuntimeException e ) {
            throw getExceptionConverter().convert( e );
        }
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

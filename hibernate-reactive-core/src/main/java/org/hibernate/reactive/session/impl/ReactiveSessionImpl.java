/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.ObjectDeletedException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.TypeMismatchException;
import org.hibernate.UnknownEntityTypeException;
import org.hibernate.UnresolvableObjectException;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.internal.ReactivePersistenceContextAdapter;
import org.hibernate.engine.spi.EffectiveEntityGraph;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.AutoFlushEvent;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.InitializeCollectionEvent;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.event.spi.LockEvent;
import org.hibernate.event.spi.MergeContext;
import org.hibernate.event.spi.MergeEvent;
import org.hibernate.event.spi.PersistContext;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.RefreshContext;
import org.hibernate.event.spi.RefreshEvent;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionImpl;
import org.hibernate.jpa.spi.NativeQueryTupleTransformer;
import org.hibernate.loader.LoaderLogging;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.loader.internal.IdentifierLoadAccessImpl;
import org.hibernate.loader.internal.LoadAccessContext;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.NaturalIdMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.query.IllegalMutationQueryException;
import org.hibernate.query.UnknownNamedQueryException;
import org.hibernate.query.criteria.JpaCriteriaInsert;
import org.hibernate.query.hql.spi.SqmQueryImplementor;
import org.hibernate.query.named.NamedResultSetMappingMemento;
import org.hibernate.query.specification.internal.MutationSpecificationImpl;
import org.hibernate.query.specification.internal.SelectionSpecificationImpl;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.query.sql.spi.NativeQueryImplementor;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.NamedSqmQueryMemento;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.query.sqm.tree.select.SqmQueryGroup;
import org.hibernate.query.sqm.tree.select.SqmQuerySpec;
import org.hibernate.query.sqm.tree.select.SqmSelectClause;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.InternalStateAssertions;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.event.ReactiveDeleteEventListener;
import org.hibernate.reactive.event.ReactiveFlushEventListener;
import org.hibernate.reactive.event.ReactiveLoadEventListener;
import org.hibernate.reactive.event.ReactiveLockEventListener;
import org.hibernate.reactive.event.ReactiveMergeEventListener;
import org.hibernate.reactive.event.ReactivePersistEventListener;
import org.hibernate.reactive.event.ReactiveRefreshEventListener;
import org.hibernate.reactive.event.impl.DefaultReactiveAutoFlushEventListener;
import org.hibernate.reactive.event.impl.DefaultReactiveInitializeCollectionEventListener;
import org.hibernate.reactive.loader.ast.spi.ReactiveNaturalIdLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
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
import org.hibernate.reactive.query.sqm.internal.ReactiveSqmQueryImpl;
import org.hibernate.reactive.query.sqm.internal.ReactiveSqmSelectionQueryImpl;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.Tuple;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CommonAbstractCriteria;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static java.lang.Boolean.TRUE;
import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.engine.spi.NaturalIdResolutions.INVALID_NATURAL_ID_REFERENCE;
import static org.hibernate.event.spi.LoadEventListener.IMMEDIATE_LOAD;
import static org.hibernate.internal.util.StringHelper.isEmpty;
import static org.hibernate.proxy.HibernateProxy.extractLazyInitializer;
import static org.hibernate.reactive.common.InternalStateAssertions.assertUseOnEventLoop;
import static org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister.forceInitialize;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.returnNullorRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * An {@link ReactiveSession} implemented by extension of
 * the {@link SessionImpl} in Hibernate core. Extension was
 * preferred to delegation because there are places where
 * Hibernate core compares the identity of session instances.
 */
public class ReactiveSessionImpl extends SessionImpl implements ReactiveSession, EventSource {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private transient final ReactiveActionQueue reactiveActionQueue = new ReactiveActionQueue( this );
	private ReactiveConnection reactiveConnection;
	private final Thread associatedWorkThread;

	//Lazily initialized
	private transient ExceptionConverter exceptionConverter;

	public ReactiveSessionImpl(
			SessionFactoryImpl delegate, SessionCreationOptions options,
			ReactiveConnection connection) {
		super( delegate, options );
		InternalStateAssertions.assertUseOnEventLoop();
		this.associatedWorkThread = Thread.currentThread();
		//matches configuration property "hibernate.jdbc.batch_size" :
		Integer batchSize = getConfiguredJdbcBatchSize();
		reactiveConnection = batchSize == null || batchSize < 2
				? connection
				: new BatchingConnection( connection, batchSize );
	}

	@Override
	public SessionImplementor getSharedContract() {
		return this;
	}

	@Override
	public Dialect getDialect() {
		threadCheck();
		return getJdbcServices().getDialect();
	}

	private void threadCheck() {
		InternalStateAssertions.assertCurrentThreadMatches( associatedWorkThread );
	}

	@Override
	protected PersistenceContext createPersistenceContext() {
		return new ReactivePersistenceContextAdapter( super.createPersistenceContext() );
	}

	@Override
	public ReactiveActionQueue getReactiveActionQueue() {
		threadCheck();
		return reactiveActionQueue;
	}

	@Override
	public Object immediateLoad(String entityName, Object id) throws HibernateException {
		throw LOG.lazyInitializationException( entityName, id );
	}

	/**
	 * Load the data for the object with the specified id into a newly created object.
	 * This is only called when lazily initializing a proxy. Do NOT return a proxy.
	 */
	@Override
	public CompletionStage<Object> reactiveImmediateLoad(String entityName, Object id)
			throws HibernateException {
		if ( LOG.isDebugEnabled() ) {
			final EntityPersister persister = requireEntityPersister( entityName );
			LOG.debugf( "Initializing proxy: %s", MessageHelper.infoString( persister, id, getFactory() ) );
		}
		threadCheck();
		final LoadEvent event = new LoadEvent(
				id, entityName, true, this,
				getReadOnlyFromLoadQueryInfluencers()
		);
		return fireLoadNoChecks( event, IMMEDIATE_LOAD )
				.thenApply( v -> {
					final Object result = event.getResult();
					final LazyInitializer lazyInitializer = extractLazyInitializer( result );
					return lazyInitializer != null ? lazyInitializer.getImplementation() : result;
				} );
	}

	@Override
	public CompletionStage<Object> reactiveInternalLoad(String entityName, Object id, boolean eager, boolean nullable) {
		final LoadEventListener.LoadType type = internalLoadType( eager, nullable );
		final EffectiveEntityGraph effectiveEntityGraph = getLoadQueryInfluencers().getEffectiveEntityGraph();
		final GraphSemantic semantic = effectiveEntityGraph.getSemantic();
		final RootGraphImplementor<?> graph = effectiveEntityGraph.getGraph();
		boolean clearedEffectiveGraph;
		if ( semantic == null || graph.appliesTo( getFactory().getJpaMetamodel().entity( entityName ) ) ) {
			clearedEffectiveGraph = false;
		}
		else {
			LOG.debug( "Clearing effective entity graph for subsequent-select" );
			clearedEffectiveGraph = true;
			effectiveEntityGraph.clear();
		}

		threadCheck();
		final LoadEvent event = makeLoadEvent( entityName, id, getReadOnlyFromLoadQueryInfluencers(), true );
		return fireLoadNoChecks( event, type )
				.thenApply( v -> {
					final Object result = event.getResult();
					if ( !nullable ) {
						UnresolvableObjectException.throwIfNull( result, id, entityName );
					}
					return result;
				} )
				.whenComplete( (v, x) -> {
					if ( clearedEffectiveGraph ) {
						effectiveEntityGraph.applyGraph( graph, semantic );
					}
				} );
	}

	@Override
	public Object load(LoadEventListener.LoadType loadType, Object id, String entityName, LockOptions lockOptions, Boolean readOnly) {
		// When the user needs a reference to the entity, we are not supposed to touche the database, and we don't return
		// a CompletionStage. So it's fine to delegate to ORM.
		// Everywhere else, reactiveLoad should be used.
		return super.load( loadType, id, entityName, lockOptions, readOnly );
	}

	/**
	 * @see SessionImpl#load(LoadEventListener.LoadType, Object, String, LockOptions, Boolean)
	 */
	public CompletionStage<Object> reactiveLoad(LoadEventListener.LoadType loadType, Object id, String entityName, LockOptions lockOptions, Boolean readOnly) {
		if ( lockOptions != null ) {
			// (from ORM) TODO: I doubt that this branch is necessary, and it's probably even wrong
			final LoadEvent event = makeLoadEvent( entityName, id, readOnly, lockOptions );
			return fireLoad( event, loadType )
					.thenApply( v -> {
						final Object result = event.getResult();
						releaseLoadEvent( event );
						return result;
					} );
		}
		else {
			final LoadEvent event = makeLoadEvent( entityName, id, readOnly, false );
			return supplyStage( () -> fireLoad( event, loadType )
					.thenApply( v -> {
						final Object result = event.getResult();
						releaseLoadEvent( event );
						if ( !loadType.isAllowNulls() && result == null ) {
							getSession().getFactory().getEntityNotFoundDelegate().handleEntityNotFound( entityName, id );
						}
						return result;
					} )
			).whenComplete( (o, throwable) -> afterOperation( throwable != null ) );
		}
	}

	@Override
	//Note: when making changes to this method, please also consider
	//      the similar code in Mutiny.fetch() and Stage.fetch()
	public <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy) {
		checkOpen();
		if ( association == null ) {
			return nullFuture();
		}
		else if ( association instanceof HibernateProxy proxy ) {
			final LazyInitializer initializer = proxy.getHibernateLazyInitializer();
			if ( !initializer.isUninitialized() ) {
				return completedFuture( unproxy ? (T) initializer.getImplementation() : association );
			}
			else {
				final String entityName = initializer.getEntityName();
				final Object identifier = initializer.getIdentifier();
				return reactiveImmediateLoad( entityName, identifier )
						.thenApply( entity -> {
							checkEntityFound( this, entityName, identifier, entity );
							initializer.setSession( this );
							initializer.setImplementation( entity );
							return unproxy ? (T) entity : association;
						} );
			}
		}
		else if (association instanceof PersistentCollection<?> collection) {
            if ( collection.wasInitialized() ) {
				return completedFuture( association );
			}
			else {
				return reactiveInitializeCollection( collection, false )
						// don't reassociate the collection instance, because
						// its owner isn't associated with this session
						.thenApply( v -> association );
			}
		}
		else if ( isPersistentAttributeInterceptable( association ) ) {
			final PersistentAttributeInterceptable interceptable = asPersistentAttributeInterceptable( association );
			final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
			if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor lazinessInterceptor ) {
                return forceInitialize( association, null, lazinessInterceptor.getIdentifier(), lazinessInterceptor.getEntityName(), this )
						.thenApply( i -> association );

			}
			else {
				return completedFuture( association );
			}
		}
		else {
			return completedFuture( association );
		}
	}

	@Override
	public <E, T> CompletionStage<T> reactiveFetch(E entity, Attribute<E, T> field) {
		final ReactiveEntityPersister entityPersister = (ReactiveEntityPersister) getEntityPersister( null, entity );
		LazyAttributeLoadingInterceptor lazyAttributeLoadingInterceptor = entityPersister.getBytecodeEnhancementMetadata()
				.extractInterceptor( entity );
		final String attributeName = field.getName();
		if ( !lazyAttributeLoadingInterceptor.isAttributeLoaded( attributeName ) ) {
			return ( (CompletionStage<T>) lazyAttributeLoadingInterceptor.fetchAttribute( entity, field.getName() ) )
					.thenApply( value -> {
						lazyAttributeLoadingInterceptor.attributeInitialized( attributeName );
						return value;
					} );
		}
		else {
			return completedFuture( (T) entityPersister.getPropertyValue( entity, attributeName ) );
		}
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(CriteriaQuery<R> criteriaQuery) {
		checkOpen();

		try {
			final SqmSelectStatement<R> selectStatement = (SqmSelectStatement<R>) criteriaQuery;
			if ( ! ( selectStatement.getQueryPart() instanceof SqmQueryGroup ) ) {
				final SqmQuerySpec<R> querySpec = selectStatement.getQuerySpec();
				final SqmSelectClause selectClause = querySpec.getSelectClause();
				if ( selectClause.getSelections().isEmpty() ) {
					if ( querySpec.getFromClause().getRoots().size() == 1 ) {
						selectClause.setSelection( querySpec.getFromClause().getRoots().get(0) );
					}
				}
			}

			return createReactiveCriteriaQuery( selectStatement, criteriaQuery.getResultType() );
		}
		catch (RuntimeException e) {
			if ( getSessionFactory().getSessionFactoryOptions().getJpaCompliance().isJpaTransactionComplianceEnabled() ) {
				markForRollbackOnly();
			}
			throw getExceptionConverter().convert( e );
		}
	}

	protected <T> ReactiveQueryImplementor<T> createReactiveCriteriaQuery(SqmStatement<T> criteria, Class<T> resultType) {
		final ReactiveSqmQueryImpl<T> query = new ReactiveSqmQueryImpl<>( criteria, resultType, this );
		applyQuerySettingsAndHints( query );
		return query;
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(TypedQueryReference<R> typedQueryReference) {
		checksBeforeQueryCreation();
		if ( typedQueryReference instanceof SelectionSpecificationImpl<R> specification ) {
			final CriteriaQuery<R> query = specification.buildCriteria( getCriteriaBuilder() );
			return new ReactiveSqmQueryImpl<>( (SqmStatement<R>) query, specification.getResultType(), this );
		}
		else if ( typedQueryReference instanceof MutationSpecificationImpl<?> specification ) {
			final CommonAbstractCriteria query = specification.buildCriteria( getCriteriaBuilder() );
			return new ReactiveSqmQueryImpl<>( (SqmStatement<R>) query, (Class<R>) specification.getResultType(), this );
		}
		else {
			@SuppressWarnings("unchecked")
			// this cast is fine because of all our impls of TypedQueryReference return Class<R>
			final Class<R> resultType = (Class<R>) typedQueryReference.getResultType();
			final ReactiveQueryImplementor<R> query = (ReactiveQueryImplementor<R>) buildNamedQuery(
					typedQueryReference.getName(),
					memento -> createSqmQueryImplementor(resultType, memento),
					memento -> createNativeQueryImplementor(resultType, memento)
			);
			typedQueryReference.getHints().forEach(query::setHint);
			return query;
		}
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(String queryString) {
		return createReactiveQuery( queryString, null );
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveQuery(String queryString, Class<R> expectedResultType) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			final HqlInterpretation<?> interpretation = interpretHql( queryString, expectedResultType );
			final ReactiveSqmQueryImpl<R> query =
					new ReactiveSqmQueryImpl<>( queryString, interpretation, expectedResultType, this );
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
	public <T> ReactiveNativeQueryImplementor<T> createReactiveNativeQuery(String sqlString) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			final ReactiveNativeQueryImpl<T> query = new ReactiveNativeQueryImpl<>( sqlString, this );
			if ( isEmpty( query.getComment() ) ) {
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
	public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultClass) {
		final ReactiveNativeQuery<T> query = createReactiveNativeQuery( sqlString );
		handleTupleResultType( resultClass, query );
		addEntityOrResultType( resultClass, query );
		return query;
	}

	private <T> void addEntityOrResultType(Class<T> resultClass, ReactiveNativeQuery<T> query) {
		if ( getFactory().getMappingMetamodel().isEntityClass( resultClass ) ) {
			query.addEntity( "alias1", resultClass.getName(), LockMode.READ );
		}
		else if ( resultClass != Object.class && resultClass != Object[].class && resultClass != Tuple.class ) {
			query.addResultTypeClass( resultClass );
		}
	}

	@Override @Deprecated(forRemoval = true)
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, Class<R> resultClass, String tableAlias) {
		final ReactiveNativeQuery<R> query = createReactiveNativeQuery( sqlString );
		if ( getFactory().getMappingMetamodel().isEntityClass( resultClass ) ) {
			query.addEntity( tableAlias, resultClass.getName(), LockMode.READ );
			return query;
		}
		else {
			throw new UnknownEntityTypeException( "unable to locate persister: " + resultClass.getName() );
		}
	}

	@Override @Deprecated(forRemoval = true)
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, String resultSetMappingName) {
		if ( isEmpty( resultSetMappingName ) ) {
			throw new IllegalArgumentException( "Result set mapping name was not specified" );
		}

		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			return new ReactiveNativeQueryImpl<>( sqlString, getResultSetMappingMemento( resultSetMappingName ), null, this );
			//TODO: why no applyQuerySettingsAndHints( query ); ???
		}
		catch (RuntimeException he) {
			throw getExceptionConverter().convert( he );
		}
	}

	@Override @Deprecated(forRemoval = true)
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, String resultSetMappingName, Class<R> resultClass) {
		final ReactiveNativeQuery<R> query = createReactiveNativeQuery( sqlString, resultSetMappingName );
		handleTupleResultType( resultClass, query );
		return query;
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString, Class<R> resultType) {
		return interpretAndCreateSelectionQuery( hqlString, resultType );
	}

	private <R> ReactiveSelectionQuery<R> interpretAndCreateSelectionQuery(String hql, Class<R> resultType) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			final HqlInterpretation<?> interpretation = interpretHql( hql, resultType );
			checkSelectionQuery( hql, interpretation );
			return createSelectionQuery( hql, resultType, interpretation );
		}
		catch (RuntimeException e) {
			markForRollbackOnly();
			throw e;
		}
	}

	private <R> ReactiveSelectionQuery<R> createSelectionQuery(String hql, Class<R> resultType, HqlInterpretation<?> interpretation) {
		final ReactiveSqmSelectionQueryImpl<R> query =
				new ReactiveSqmSelectionQueryImpl<>( hql, interpretation, resultType, this );
		if ( resultType != null ) {
			checkResultType( resultType, query );
		}
		query.setComment( hql );
		applyQuerySettingsAndHints( query );
		return query;
	}

	@Override
	public <R> ReactiveQueryImplementor<R> createReactiveNamedQuery(String name) {
		checksBeforeQueryCreation();
		try {
			return (ReactiveQueryImplementor<R>) buildNamedQuery(
					name,
					this::createSqmQueryImplementor,
					this::createNativeQueryImplementor
			);
		}
		catch (RuntimeException e) {
			throw convertNamedQueryException( e );
		}
	}

	@Override
	public <R> ReactiveQueryImplementor<R> createReactiveNamedQuery(String name, Class<R> resultType) {
		checksBeforeQueryCreation();
		if ( resultType == null ) {
			throw new IllegalArgumentException( "Result class is null" );
		}
		try {
			return buildNamedQuery(
					name,
					memento -> createReactiveSqmQueryImplementor( resultType, memento ),
					memento -> createReactiveNativeQueryImplementor( resultType, memento )
			);
		}
		catch (RuntimeException e) {
			throw convertNamedQueryException( e );
		}
	}

	private void checksBeforeQueryCreation() {
		checkOpen();
		checkTransactionSynchStatus();
	}

	protected <T> ReactiveNativeQueryImpl<T> createReactiveNativeQueryImplementor(Class<T> resultType, NamedNativeQueryMemento<?> memento) {
		final NativeQueryImplementor<T> query = memento.toQuery(this, resultType );
		if ( isEmpty( query.getComment() ) ) {
			query.setComment( "dynamic native SQL query" );
		}
		applyQuerySettingsAndHints( query );
		return (ReactiveNativeQueryImpl<T>) query;
	}

	protected <T> ReactiveSqmQueryImpl<T> createReactiveSqmQueryImplementor(Class<T> resultType, NamedSqmQueryMemento<?> memento) {
		final SqmQueryImplementor<T> query = memento.toQuery( this, resultType );
		if ( isEmpty( query.getComment() ) ) {
			query.setComment( "dynamic query" );
		}
		applyQuerySettingsAndHints( query );
		if ( memento.getLockOptions() != null ) {
			query.setLockOptions( memento.getLockOptions() );
		}
		return (ReactiveSqmQueryImpl<T>) query;
	}

	private RuntimeException convertNamedQueryException(RuntimeException e) {
		if ( e instanceof UnknownNamedQueryException ) {
			// JPA expects this to mark the transaction for rollback only
			getTransactionCoordinator().getTransactionDriverControl().markRollbackOnly();
			// it also expects an IllegalArgumentException, so wrap UnknownNamedQueryException
			return new IllegalArgumentException( e.getMessage(), e );
		}
		else if ( e instanceof IllegalArgumentException ) {
			return e;
		}
		else {
			return getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(CriteriaQuery<R> criteria) {
		SqmUtil.verifyIsSelectStatement( (SqmStatement<R>) criteria, null );
		return new ReactiveSqmSelectionQueryImpl<>( (SqmSelectStatement<R>) criteria, criteria.getResultType(), this );
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(String hqlString) {
		final QueryImplementor<?> query = createQuery( hqlString );
		final SqmStatement<R> sqmStatement = ( (SqmQueryImplementor<R>) query ).getSqmStatement();
		checkMutationQuery( hqlString, sqmStatement );
		return new ReactiveSqmQueryImpl<>( sqmStatement, null, this );
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaUpdate<R> updateQuery) {
		checkOpen();
		try {
			return createReactiveCriteriaQuery( (SqmUpdateStatement<R>) updateQuery, null );
		}
		catch ( RuntimeException e ) {
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaDelete<R> deleteQuery) {
		checkOpen();
		try {
			return createReactiveCriteriaQuery( (SqmDeleteStatement<R>) deleteQuery, null );
		}
		catch ( RuntimeException e ) {
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(JpaCriteriaInsert<R> insert) {
		checkOpen();
		try {
			return createReactiveCriteriaQuery( (SqmInsertStatement<R>) insert, null );
		}
		catch ( RuntimeException e ) {
			throw getExceptionConverter().convert( e );
		}
	}

	@Override
	public <R> ReactiveMutationQuery<R> createNamedReactiveMutationQuery(String queryName) {
		return (ReactiveMutationQuery<R>) buildNamedQuery(
				queryName,
				memento -> createSqmQueryImplementor( queryName, memento ),
				memento -> createNativeQueryImplementor( queryName, memento )
		);
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String queryName) {
		return (ReactiveSelectionQuery<R>) createNamedSelectionQuery( queryName, null );
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
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String queryString, AffectedEntities affectedEntities) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			final ReactiveNativeQueryImpl<R> query = new ReactiveNativeQueryImpl<>( queryString, null, this );
			addAffectedEntities( affectedEntities, query );
			if ( isEmpty( query.getComment() ) ) {
				query.setComment( "dynamic native SQL query" );
			}
			applyQuerySettingsAndHints( query );
			return query;
		}
		catch (RuntimeException he) {
			throw getExceptionConverter().convert( he );
		}
	}

	private void addAffectedEntities(AffectedEntities affectedEntities, NativeQueryImplementor<?> query) {
		for ( String space : affectedEntities.getAffectedSpaces( getFactory() ) ) {
			query.addSynchronizedQuerySpace( space );
		}
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities) {
		final ReactiveNativeQuery<R> query = createReactiveNativeQuery( queryString, affectedEntities );
		handleTupleResultType( resultType, query );
		addEntityOrResultType( resultType, query );
		return query;
	}

	private static <R> void handleTupleResultType(Class<R> resultType, ReactiveNativeQuery<R> query) {
		if ( Tuple.class.equals(resultType) ) {
			query.setTupleTransformer( NativeQueryTupleTransformer.INSTANCE );
		}
	}

	@Override
	public <R> ReactiveNativeQueryImpl<R> createReactiveNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();
		// Should we throw an exception?
		NamedResultSetMappingMemento memento = resultSetMapping == null ? null : getResultSetMappingMemento( resultSetMapping.getName() );
		try {
			// Same approach as AbstractSharedSessionContract#createNativeQuery(String, String)
			final ReactiveNativeQueryImpl<R> nativeQuery = new ReactiveNativeQueryImpl<>( queryString, memento, null, this );
			applyQuerySettingsAndHints( nativeQuery );
			return nativeQuery;
		}
		catch ( RuntimeException he ) {
			throw getExceptionConverter().convert( he );
		}
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(
			String queryString,
			ResultSetMapping<R> resultSetMapping,
			AffectedEntities affectedEntities) {
		final ReactiveNativeQueryImpl<R> nativeQuery = createReactiveNativeQuery( queryString, resultSetMapping );
		addAffectedEntities( affectedEntities, nativeQuery );
		return nativeQuery;
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		final NamedResultSetMappingMemento mapping = getResultSetMappingMemento( mappingName );
		if ( mapping == null ) {
			throw new IllegalArgumentException( "result set mapping does not exist: " + mappingName );
		}

		return new ResultSetMapping<>() {
			@Override
			public String getName() {
				return mappingName;
			}

			@Override
			public Class<T> getResultType() {
				return resultType;
			}
		};
	}

	/**
	 * @deprecated use {@link #reactiveInitializeCollection(PersistentCollection, boolean)} instead
	 */
	@Deprecated
	@Override
	public void initializeCollection(PersistentCollection<?> collection, boolean writing) {
		throw LOG.collectionCannotBeInitializedlazyInitializationException( collectionRoleLogMessage( collection ) );
	}

	private static String collectionRoleLogMessage(PersistentCollection<?> collection) {
		if ( collection == null ) {
			return "collection is null";
		}
		return collection.getRole();
	}

	@Override
	public CompletionStage<Void> reactiveInitializeCollection(PersistentCollection<?> collection, boolean writing) {
		checkOpenOrWaitingForAutoClose();
		pulseTransactionCoordinator();
		InitializeCollectionEvent event = new InitializeCollectionEvent( collection, this );

        return getFactory().getEventListenerGroups().eventListenerGroup_INIT_COLLECTION
				.fireEventOnEachListener(
						event,
						(DefaultReactiveInitializeCollectionEventListener l) -> l::onReactiveInitializeCollection
				)
				.handle( (v, e) -> {
					delayedAfterCompletion();
					if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public CompletionStage<Void> reactivePersist(Object entity) {
		checkOpen();
		return firePersist( new PersistEvent( null, entity, this ) );
	}

	@Override
	public CompletionStage<Void> reactivePersist(String entityName, Object entity) {
		checkOpen();
		return firePersist( new PersistEvent( entityName, entity, this ) );
	}

	@Override
	public CompletionStage<Void> reactivePersist(Object object, PersistContext copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersist( copiedAlready, new PersistEvent( null, object, this ) );
	}

	// Should be similar to firePersist
	private CompletionStage<Void> firePersist(PersistEvent event) {
		checkTransactionSynchStatus();
		checkNoUnresolvedActionsBeforeOperation();

		return getFactory().getEventListenerGroups().eventListenerGroup_PERSIST
				.fireEventOnEachListener( event, (ReactivePersistEventListener l) -> l::reactiveOnPersist )
				.handle( (v, e) -> {
					checkNoUnresolvedActionsAfterOperation();

					if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	private CompletionStage<Void> firePersist(PersistContext copiedAlready, PersistEvent event) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_PERSIST
				.fireEventOnEachListener( event, copiedAlready, (ReactivePersistEventListener l) -> l::reactiveOnPersist )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public CompletionStage<Void> reactivePersistOnFlush(Object entity, PersistContext copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersistOnFlush( copiedAlready, new PersistEvent( null, entity, this ) );
	}

	private CompletionStage<Void> firePersistOnFlush(PersistContext copiedAlready, PersistEvent event) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_PERSIST
				.fireEventOnEachListener( event, copiedAlready, (ReactivePersistEventListener l) -> l::reactiveOnPersist )
				.whenComplete( (v, e) -> delayedAfterCompletion() );
	}

	@Override
	public CompletionStage<Void> reactiveRemove(Object entity) {
		checkOpen();
		return fireRemove( new DeleteEvent( entity, this ) );
	}

	@Override
	public CompletionStage<Void> reactiveRemove(
			String entityName,
			Object child,
			boolean isCascadeDeleteEnabled,
			DeleteContext transientEntities) {
		checkOpenOrWaitingForAutoClose();
		final boolean removingOrphanBeforeUpdates = persistenceContext().isRemovingOrphanBeforeUpdates();
		if ( LOG.isTraceEnabled() && removingOrphanBeforeUpdates ) {
			logRemoveOrphanBeforeUpdates( "before continuing", entityName, entityName );
		}

		return fireRemove(
				new DeleteEvent( entityName, child, isCascadeDeleteEnabled, removingOrphanBeforeUpdates, this ),
				transientEntities
		);
	}

	private ReactivePersistenceContextAdapter persistenceContext() {
		return (ReactivePersistenceContextAdapter) getPersistenceContextInternal();
	}

	private void logRemoveOrphanBeforeUpdates(String timing, String entityName, Object entity) {
		if ( LOG.isTraceEnabled() ) {
			final EntityEntry entityEntry = persistenceContext().getEntry( entity );
			LOG.tracef( "%s remove orphan before updates: [%s]", timing,
						entityEntry == null ? entityName : MessageHelper.infoString( entityName, entityEntry.getId() )
			);
		}
	}

	// Should be similar to fireRemove
	private CompletionStage<Void> fireRemove(DeleteEvent event) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_DELETE
				.fireEventOnEachListener( event, (ReactiveDeleteEventListener l) -> l::reactiveOnDelete )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof ObjectDeletedException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if ( e instanceof RuntimeException ) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	private CompletionStage<Void> fireRemove(DeleteEvent event, DeleteContext transientEntities) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_DELETE
				.fireEventOnEachListener( event, transientEntities, (ReactiveDeleteEventListener l) -> l::reactiveOnDelete )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof ObjectDeletedException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if ( e instanceof RuntimeException ) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public <T> CompletionStage<T> reactiveMerge(T object) throws HibernateException {
		checkOpen();
		return fireMerge( new MergeEvent( null, object, this ) );
	}

	@Override
	public CompletionStage<Void> reactiveMerge(Object object, MergeContext copiedAlready)
			throws HibernateException {
		checkOpenOrWaitingForAutoClose();
		return fireMerge( copiedAlready, new MergeEvent( null, object, this ) );
	}

	@SuppressWarnings("unchecked")
	private <T> CompletionStage<T> fireMerge(MergeEvent event) {
		checkTransactionSynchStatus();
		checkNoUnresolvedActionsBeforeOperation();

		return getFactory().getEventListenerGroups().eventListenerGroup_MERGE
				.fireEventOnEachListener( event, (ReactiveMergeEventListener l) -> l::reactiveOnMerge )
				.handle( (v, e) -> {
					checkNoUnresolvedActionsAfterOperation();

					if ( e instanceof ObjectDeletedException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if ( e instanceof RuntimeException ) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnOrRethrow( e, (T) event.getResult() );
				} );
	}

	private CompletionStage<Void> fireMerge(MergeContext copiedAlready, MergeEvent event) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_MERGE
				.fireEventOnEachListener( event, copiedAlready,(ReactiveMergeEventListener l) -> l::reactiveOnMerge )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof ObjectDeletedException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if ( e instanceof RuntimeException ) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public CompletionStage<Void> reactiveFlush() {
		checkOpen();
		return doFlush();
	}

	@Override
	public CompletionStage<Void> reactiveAutoflush() {
		return getHibernateFlushMode().lessThan( FlushMode.COMMIT ) ? voidFuture() : doFlush();
	}

	@Override
	public CompletionStage<Boolean> reactiveAutoFlushIfRequired(Set<String> querySpaces) {
		checkOpen();
		// FIXME: Can't we implement this part?
//		if ( !isTransactionInProgress() ) {
//			// do not auto-flush while outside a transaction
//			return CompletionStages.falseFuture();
//		}

		AutoFlushEvent event = new AutoFlushEvent( querySpaces, this );
		return getFactory().getEventListenerGroups().eventListenerGroup_AUTO_FLUSH
				.fireEventOnEachListener( event, (DefaultReactiveAutoFlushEventListener l) -> l::reactiveOnAutoFlush )
				.thenApply( v -> event.isFlushRequired() );
	}

	@Override
	public CompletionStage<Void> reactiveForceFlush(EntityEntry entry) {
		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Flushing to force deletion of re-saved object: %s",
					MessageHelper.infoString( entry.getPersister(), entry.getId(), getFactory() )
			);
		}

		if ( getPersistenceContextInternal().getCascadeLevel() > 0 ) {
			return failedFuture( new ObjectDeletedException(
					"deleted object would be re-saved by cascade (remove deleted object from associations)",
					entry.getId(),
					entry.getPersister().getEntityName()
			) );
		}
		checkOpenOrWaitingForAutoClose();
		return doFlush();
	}

	private CompletionStage<Void> doFlush() {
		checkTransactionNeededForUpdateOperation( "no transaction is in progress" );
		pulseTransactionCoordinator();

		if ( getPersistenceContextInternal().getCascadeLevel() > 0 ) {
			throw LOG.flushDuringCascadeIsDangerous();
		}

		return getFactory().getEventListenerGroups().eventListenerGroup_FLUSH
				.fireEventOnEachListener( new FlushEvent( this ), (ReactiveFlushEventListener l) -> l::reactiveOnFlush )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof CompletionException ) {
						if ( e.getCause() instanceof RuntimeException ) {
							e = getExceptionConverter().convert( (RuntimeException) e.getCause() );
						}
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public ExceptionConverter getExceptionConverter() {
		if ( exceptionConverter == null ) {
			exceptionConverter = new ReactiveExceptionConverter( this );
		}
		return exceptionConverter;
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object entity, LockOptions lockOptions) {
		checkOpen();
		return fireRefresh( new RefreshEvent( entity, lockOptions, this ) );
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode) {
		return reactiveRefresh( entity, toLockOptions( lockMode ) );
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object object, RefreshContext refreshedAlready) {
		checkOpenOrWaitingForAutoClose();
		return fireRefresh( refreshedAlready, new RefreshEvent( null, object, this ) );
	}

	CompletionStage<Void> fireRefresh(RefreshEvent event) {
		if ( !getSessionFactory().getSessionFactoryOptions().isAllowRefreshDetachedEntity() ) {
			if ( event.getEntityName() != null ) {
				if ( !contains( event.getEntityName(), event.getObject() ) ) {
					throw new IllegalArgumentException( "Entity not managed" );
				}
			}
			else {
				if ( !contains( event.getObject() ) ) {
					throw new IllegalArgumentException( "Entity not managed" );
				}
			}
		}
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_REFRESH
				.fireEventOnEachListener( event, (ReactiveRefreshEventListener l) -> l::reactiveOnRefresh )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof RuntimeException ) {
						if ( !getSessionFactory().getSessionFactoryOptions().isJpaBootstrap() ) {
							if ( e instanceof HibernateException ) {
								return rethrow( e );
							}
						}
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	private CompletionStage<Void> fireRefresh(RefreshContext refreshedAlready, RefreshEvent event) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_REFRESH
				.fireEventOnEachListener( event, refreshedAlready, (ReactiveRefreshEventListener l) -> l::reactiveOnRefresh )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public CompletionStage<Void> reactiveLock(Object object, LockOptions lockOptions) {
		checkOpen();
		return fireLock( new LockEvent( object, lockOptions, this ) );
	}

	@Override
	public CompletionStage<Void> reactiveLock(Object entity, LockMode lockMode) {
		return reactiveLock( entity, toLockOptions( lockMode ) );
	}

	@Override
	public CompletionStage<Void> reactiveLock(String entityName, Object object, LockOptions lockOptions) {
		checkOpen();
		return fireLock( new LockEvent( entityName, object, lockOptions, this ) );
	}

	private CompletionStage<Void> fireLock(LockEvent event) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_LOCK
				.fireEventOnEachListener( event, (ReactiveLockEventListener l) -> l::reactiveOnLock )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(Class<T> entityClass, Object id) {
		return reactiveById( entityClass ).load( id );
	}

	private <T> ReactiveIdentifierLoadAccessImpl<T> reactiveById(Class<T> entityClass) {
		return new ReactiveIdentifierLoadAccessImpl<>( this, requireEntityPersister( entityClass ) );
	}

	private <T> ReactiveIdentifierLoadAccessImpl<T> reactiveById(String entityName) {
		return new ReactiveIdentifierLoadAccessImpl<>( this, requireEntityPersister( entityName ) );
	}

	@Override
	public <T> CompletionStage<T> reactiveFind(
			Class<T> entityClass,
			Object id,
			LockOptions lockOptions,
			EntityGraph<T> fetchGraph) {
		checkOpen();
		return supplyStage( () -> {
			if ( fetchGraph != null ) {
				getLoadQueryInfluencers()
						.getEffectiveEntityGraph()
						.applyGraph( (RootGraphImplementor<T>) fetchGraph, GraphSemantic.FETCH );
			}
			getLoadQueryInfluencers().setReadOnly( readOnlyHint( null ) );

			return reactiveById( entityClass )
					.with( determineAppropriateLocalCacheMode( null ) )
					.with( lockOptions )
					.load( id );
		} ).handle( CompletionStages::handle )
				.thenCompose( handler -> handleReactiveFindException( entityClass, id, lockOptions, handler ) )
				.whenComplete( (v, e) -> {
					getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
					getLoadQueryInfluencers().setReadOnly( null );
				} );
	}

	@Override
	public <T> CompletionStage<T> reactiveFind(Class<T> entityClass, Object id, LockMode lockMode, EntityGraph<T> fetchGraph){
		return reactiveFind( entityClass, id, toLockOptions( lockMode ), fetchGraph );
	}

	private <T> CompletionStage<T> handleReactiveFindException(
			Class<T> entityClass,
			Object primaryKey,
			LockOptions lockOptions,
			CompletionStages.CompletionStageHandler<T, Throwable> handler) {
		if ( !handler.hasFailed() ) {
			return handler.getResultAsCompletionStage();
		}
		final Throwable e = handler.getThrowable();
		if ( e instanceof EntityNotFoundException ) {
			// We swallow other sorts of EntityNotFoundException and return null
			// For example, DefaultLoadEventListener.proxyImplementation() throws
			// EntityNotFoundException if there's an existing proxy in the session,
			// but the underlying database row has been deleted (see HHH-7861)
			logIgnoringEntityNotFound( entityClass, primaryKey );
			return nullFuture();
		}
		if ( e instanceof ObjectDeletedException ) {
			// the spec is silent about people doing remove() find() on the same PC
			return null;
		}
		if ( e instanceof ObjectNotFoundException ) {
			// should not happen on the entity itself with get
			// TODO: in fact this will occur instead of EntityNotFoundException
			//       when using StandardEntityNotFoundDelegate, so probably we
			//       should return null here, as we do above
			return failedFuture( new IllegalArgumentException( e.getMessage(), e ) );
		}
		if ( e instanceof MappingException || e instanceof TypeMismatchException || e instanceof ClassCastException ) {
			return failedFuture( getExceptionConverter().convert( new IllegalArgumentException(
					e.getMessage(),
					e
			) ) );
		}
		if ( e instanceof JDBCException ) {
			// I don't think this is ever going to happen in Hibernate Reactive
			if ( accessTransaction().isActive() && accessTransaction().getRollbackOnly() ) {
				// Assume situation HHH-12472 running on WildFly
				// Just log the exception and return null
				LOG.jdbcExceptionThrownWithTransactionRolledBack( (JDBCException) e );
				return nullFuture();
			}
			else {
				return failedFuture( getExceptionConverter().convert( (JDBCException) e, lockOptions ) );
			}
		}
		if ( e instanceof RuntimeException ) {
			return failedFuture( getExceptionConverter().convert( (RuntimeException) e, lockOptions ) );
		}

		return handler.getResultAsCompletionStage();
	}

	@Override
	public <T> CompletionStage<List<T>> reactiveFind(Class<T> entityClass, Object... ids) {
		return new ReactiveMultiIdentifierLoadAccessImpl<>( entityClass ).multiLoad( ids );
	}

	@Override
	public <T> CompletionStage<T> reactiveFind(Class<T> entityClass, Map<String, Object> ids) {
		final ReactiveEntityPersister persister = entityPersister( entityClass );
		final Object normalizedIdValues = persister.getNaturalIdMapping().normalizeInput( ids );
		return new NaturalIdLoadAccessImpl<T>( this, persister, requireEntityPersister( entityClass ) )
				.resolveNaturalId( normalizedIdValues )
				.thenCompose( id -> reactiveFind( entityClass, id ) );
	}

	private <T> ReactiveEntityPersister entityPersister(Class<T> entityClass) {
		return (ReactiveEntityPersister) getFactory().getMappingMetamodel().getEntityDescriptor( entityClass );
	}

	private CompletionStage<Void> fireLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
		checkOpenOrWaitingForAutoClose();
		return fireLoadNoChecks( event, loadType )
				.thenAccept( v -> delayedAfterCompletion() );
	}

	/**
	 * This version of {@link #load} is for use by internal methods only.
	 * It skips the session open check, transaction sync checks, and so on,
	 * which have been shown to be expensive (apparently they prevent these
	 * hot methods from being inlined).
	 */
	private CompletionStage<Void> fireLoadNoChecks(LoadEvent event, LoadEventListener.LoadType loadType) {
		pulseTransactionCoordinator();

		return getFactory().getEventListenerGroups().eventListenerGroup_LOAD
				.fireEventOnEachListener( event, loadType,(ReactiveLoadEventListener l) -> l::reactiveOnLoad
		);
	}

	@Override
	public void delayedAfterCompletion() {
		//disable for now, but figure out what to do here
	}

	public void afterOperation(boolean success) {
		//disable for now, but figure out what to do here
	}

	@Override
	public void checkTransactionNeededForUpdateOperation(String exceptionMessage) {
		//no-op because we don't support transactions
	}

	private class ReactiveIdentifierLoadAccessImpl<T> extends IdentifierLoadAccessImpl<CompletionStage<T>> {

		public ReactiveIdentifierLoadAccessImpl(LoadAccessContext context, EntityPersister entityPersister) {
			super(context, entityPersister);
		}

		@Override
		protected CompletionStage<T> perform(Supplier<CompletionStage<T>> executor) {
			if ( getGraphSemantic() != null ) {
				if ( getRootGraph() == null ) {
					throw new IllegalArgumentException( "Graph semantic specified, but no RootGraph was supplied" );
				}
			}
			CacheMode sessionCacheMode = getCacheMode();
			boolean cacheModeChanged = false;
			if ( getCacheMode() != null ) {
				// naive check for now...
				// todo : account for "conceptually equal"
				if ( getCacheMode() != sessionCacheMode ) {
					setCacheMode( getCacheMode() );
					cacheModeChanged = true;
				}
			}

			if ( getGraphSemantic() != null ) {
				getLoadQueryInfluencers().getEffectiveEntityGraph().applyGraph( getRootGraph(), getGraphSemantic() );
			}

			boolean finalCacheModeChanged = cacheModeChanged;
			return executor.get()
					.whenComplete( (v, x) -> {
						if ( getGraphSemantic() != null ) {
							getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
						}
						if ( finalCacheModeChanged ) {
							// change it back
							setCacheMode( sessionCacheMode );
						}
					} );
		}

		@Override
		protected CompletionStage<T> doGetReference(Object id) {
			// getReference si supposed to return T, not CompletionStage<T>
			// I can't think of a way to change the super class so that it is mapped correctly.
			// So, for now, I will throw an exception and make sure that it doesn't really get called
			// (the one in SessionImpl should be used)
			throw new UnsupportedOperationException();
		}

		@Override
		@SuppressWarnings("unchecked")
		protected CompletionStage<T> doLoad(Object id) {
			if ( id == null ) {
				// This is needed to make the tests with NaturalIds pass.
				// It's was already part of Hibernate Reactive.
				// I'm not sure why though, it doesn't seem like Hibernate ORM does it.
				return nullFuture();
			}
			final ReactiveSession session = (ReactiveSession) getContext().getSession();
			return supplyStage( () -> session
					.reactiveLoad( LoadEventListener.GET, coerceId( id, session.getFactory() ), getEntityPersister().getEntityName(), getLockOptions(), isReadOnly( getContext().getSession() ) )
			)
					.handle( CompletionStages::handle )
					.thenCompose( handler -> handler.getThrowable() instanceof ObjectNotFoundException
							? nullFuture()
							: handler.getResultAsCompletionStage()
					)
					.thenApply( result -> {
						// ORM calls
						// initializeIfNecessary( result );
						// But, Hibernate Reactive doesn't support lazy initializations
						return (T) result;
					} );
		}
	}

	private class ReactiveMultiIdentifierLoadAccessImpl<T> implements MultiIdLoadOptions {
		private final ReactiveEntityPersister entityPersister;

		private LockOptions lockOptions;
		private CacheMode cacheMode;

		private RootGraphImplementor<T> rootGraph;
		private GraphSemantic graphSemantic;

		private Integer batchSize;
		private boolean sessionCheckingEnabled;
		private boolean returnOfDeletedEntitiesEnabled;
		private boolean orderedReturnEnabled = true;
		private boolean readOnly;

		public ReactiveMultiIdentifierLoadAccessImpl(EntityPersister entityPersister) {
			this.entityPersister = (ReactiveEntityPersister) entityPersister;
		}

		@Override
		public Boolean getReadOnly(SessionImplementor session) {
			return session.getLoadQueryInfluencers().getReadOnly();
		}

		public ReactiveMultiIdentifierLoadAccessImpl(Class<T> entityClass) {
			this( getFactory().getMappingMetamodel().getEntityDescriptor( entityClass ) );
		}

		@Override
		public LockOptions getLockOptions() {
			return lockOptions;
		}

		public final ReactiveMultiIdentifierLoadAccessImpl<T> with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		public ReactiveMultiIdentifierLoadAccessImpl<T> with(CacheMode cacheMode) {
			this.cacheMode = cacheMode;
			return this;
		}

		public ReactiveMultiIdentifierLoadAccessImpl<T> with(RootGraph<T> graph, GraphSemantic semantic) {
			this.rootGraph = (RootGraphImplementor<T>) graph;
			this.graphSemantic = semantic;
			return this;
		}

		@Override
		public Integer getBatchSize() {
			return batchSize;
		}

		public ReactiveMultiIdentifierLoadAccessImpl<T> withBatchSize(int batchSize) {
            this.batchSize = batchSize < 1 ? null : batchSize;
			return this;
		}

		@Override
		public boolean isSessionCheckingEnabled() {
			return sessionCheckingEnabled;
		}

		@Override
		public boolean isSecondLevelCacheCheckingEnabled() {
			return cacheMode == CacheMode.NORMAL || cacheMode == CacheMode.GET;
		}

		public ReactiveMultiIdentifierLoadAccessImpl<T> enableSessionCheck(boolean enabled) {
			this.sessionCheckingEnabled = enabled;
			return this;
		}

		@Override
		public boolean isReturnOfDeletedEntitiesEnabled() {
			return returnOfDeletedEntitiesEnabled;
		}

		public ReactiveMultiIdentifierLoadAccessImpl<T> enableReturnOfDeletedEntities(boolean enabled) {
			this.returnOfDeletedEntitiesEnabled = enabled;
			return this;
		}

		@Override
		public boolean isOrderReturnEnabled() {
			return orderedReturnEnabled;
		}

		public ReactiveMultiIdentifierLoadAccessImpl<T> enableOrderedReturn(boolean enabled) {
			this.orderedReturnEnabled = enabled;
			return this;
		}

		public CompletionStage<List<T>> multiLoad(Object... ids) {
			Object[] sids = new Object[ids.length];
			System.arraycopy( ids, 0, sids, 0, ids.length );

			return perform( () -> {
				final CompletionStage<? extends List<?>> stage =
						entityPersister.reactiveMultiLoad( sids, ReactiveSessionImpl.this, this );
				return (CompletionStage<List<T>>) stage;
            });
		}

		public CompletionStage<List<T>> perform(Supplier<CompletionStage<List<T>>> executor) {
			CacheMode sessionCacheMode = getCacheMode();
			boolean cacheModeChanged = false;
			if ( cacheMode != null ) {
				// naive check for now...
				// todo : account for "conceptually equal"
				if ( cacheMode != sessionCacheMode ) {
					setCacheMode( cacheMode );
					cacheModeChanged = true;
				}
			}

			if ( graphSemantic != null ) {
				if ( rootGraph == null ) {
					throw new IllegalArgumentException( "Graph semantic specified, but no RootGraph was supplied" );
				}
				getLoadQueryInfluencers().getEffectiveEntityGraph().applyGraph( rootGraph, graphSemantic );
			}

			boolean finalCacheModeChanged = cacheModeChanged;
			return executor.get()
					.whenComplete( (v, x) -> {
						if ( graphSemantic != null ) {
							getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
						}
						if ( finalCacheModeChanged ) {
							// change it back
							setCacheMode( sessionCacheMode );
						}
					} );
		}
	}

	private class NaturalIdLoadAccessImpl<T> {
		private final LoadAccessContext context;
		private final ReactiveEntityPersister entityPersister;
		private final EntityMappingType entityDescriptor;
		private LockOptions lockOptions;
		private boolean synchronizationEnabled = true;

		private NaturalIdLoadAccessImpl(LoadAccessContext context, ReactiveEntityPersister entityPersister, EntityMappingType entityDescriptor) {
			this.context = context;
			this.entityPersister = entityPersister;
			this.entityDescriptor = entityDescriptor;

			if ( !entityPersister.hasNaturalIdentifier() ) {
				throw LOG.entityDidNotDefinedNaturalId( entityPersister.getEntityName() );
			}
		}

		public NaturalIdLoadAccessImpl<T> with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		/**
		 * @see org.hibernate.loader.internal.BaseNaturalIdLoadAccessImpl#doGetReference(Object)
		 */
		protected final CompletionStage<Object> resolveNaturalId(Object normalizedNaturalIdValue) {
			performAnyNeededCrossReferenceSynchronizations();

			context.checkOpenOrWaitingForAutoClose();
			context.pulseTransactionCoordinator();

			final SessionImplementor session = getSession();
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			final Object cachedResolution = persistenceContext.getNaturalIdResolutions()
					.findCachedIdByNaturalId( normalizedNaturalIdValue, entityPersister() );
			if ( cachedResolution == INVALID_NATURAL_ID_REFERENCE ) {
				// the entity is deleted, although not yet flushed - return null
				return nullFuture();
			}
			else if ( cachedResolution != null ) {
				return completedFuture( cachedResolution );
			}
			else {
				LoaderLogging.LOADER_LOGGER.debugf(
						"Selecting entity identifier by natural-id for `#getReference` handling - %s : %s",
						entityPersister().getEntityName(),
						normalizedNaturalIdValue
				);
				return ( (ReactiveNaturalIdLoader) entityPersister().getNaturalIdLoader() )
						.resolveNaturalIdToId( normalizedNaturalIdValue, session );
			}
		}

		protected void performAnyNeededCrossReferenceSynchronizations() {
			if ( !synchronizationEnabled ) {
				// synchronization (this process) was disabled
				return;
			}

			final NaturalIdMapping naturalIdMapping = entityDescriptor.getNaturalIdMapping();
			if ( !naturalIdMapping.isMutable() ) {
				// only mutable natural-ids need this processing
				return;
			}
			if ( !isTransactionInProgress() ) {
				// not in a transaction so skip synchronization
				return;
			}

			final PersistenceContext persistenceContext = getPersistenceContextInternal();
			final boolean loggerDebugEnabled = LoaderLogging.LOADER_LOGGER.isDebugEnabled();
			for ( Object pk : persistenceContext.getNaturalIdResolutions()
					.getCachedPkResolutions( entityPersister ) ) {
				final EntityKey entityKey = generateEntityKey( pk, entityPersister );
				final Object entity = persistenceContext.getEntity( entityKey );
				final EntityEntry entry = persistenceContext.getEntry( entity );

				if ( entry == null ) {
					if ( loggerDebugEnabled ) {
						LoaderLogging.LOADER_LOGGER.debugf(
								"Cached natural-id/pk resolution linked to null EntityEntry in persistence context : %s#%s",
								entityDescriptor.getEntityName(),
								pk
						);
					}
					continue;
				}

				if ( !entry.requiresDirtyCheck( entity ) ) {
					continue;
				}

				// MANAGED is the only status we care about here...
				if ( entry.getStatus() != Status.MANAGED ) {
					continue;
				}

				persistenceContext.getNaturalIdResolutions().handleSynchronization( pk, entity, entityPersister() );
			}
		}

		protected ReactiveEntityPersister entityPersister() {
			return entityPersister;
		}
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		if ( ReactiveSession.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( this );
		}
		return super.unwrap( clazz );
	}

	public ReactiveConnection getReactiveConnection() {
		assertUseOnEventLoop();
		return reactiveConnection;
	}

	@Override
	public void close() throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveClose()" );
	}

	@Override
	public CompletionStage<Void> reactiveClose() {
		try {
			super.close();
			return closeConnection();
		}
		catch (RuntimeException e) {
			return closeConnection()
					.handle( CompletionStages::handle )
					.thenCompose( closeConnectionHandler -> {
						if ( closeConnectionHandler.hasFailed() ) {
							LOG.errorClosingConnection( closeConnectionHandler.getThrowable() );
						}
						return failedFuture( e );
					} );
		}
	}

	private CompletionStage<Void> closeConnection() {
		return reactiveConnection != null
				? reactiveConnection.close()
				: voidFuture();
	}

	@Override
	public Integer getBatchSize() {
		return getJdbcBatchSize();
	}

	@Override
	public void setBatchSize(Integer batchSize) {
		setJdbcBatchSize( batchSize );
		reactiveConnection = reactiveConnection.withBatchSize( batchSize );
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Class<T> getEntityClass(T entity) {
		if ( entity instanceof HibernateProxy proxy ) {
			return (Class<T>) proxy.getHibernateLazyInitializer()
					.getPersistentClass();
		}
		else {
			return (Class<T>) getEntityPersister( null, entity )
					.getMappedClass();
		}
	}

	@Override
	public Object getEntityId(Object entity) {
		if ( entity instanceof HibernateProxy proxy ) {
			return proxy.getHibernateLazyInitializer()
					.getIdentifier();
		}
		else {
			return getEntityPersister( null, entity )
					.getIdentifier( entity, this );
		}
	}

	@Override
	public void checkOpen() {
		//The checkOpen check is invoked on all most used public API, making it an
		//excellent hook to also check for the right thread to be used
		//(which is an assertion so costs us nothing in terms of performance, after inlining).
		threadCheck();
		super.checkOpen();
	}

	@Override
	public void removeOrphanBeforeUpdates(String entityName, Object child) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveRemoveOrphanBeforeUpdates(String entityName, Object child) {
		// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
		// ordering is improved.
		final PersistenceContext persistenceContext = getPersistenceContextInternal();
		persistenceContext.beginRemoveOrphanBeforeUpdates();
		return fireRemove( new DeleteEvent( entityName, child, false, true, this ) )
				.thenAccept( v -> {
					persistenceContext.endRemoveOrphanBeforeUpdates();
					if ( LOG.isTraceEnabled() ) {
						logRemoveOrphanBeforeUpdates( "end", entityName, child, persistenceContext );
					}
				} );
	}

	@Override
	public void clear() {
		super.clear();
		this.reactiveActionQueue.clear();
	}

	private void logRemoveOrphanBeforeUpdates(
			String timing,
			String entityName,
			Object entity,
			PersistenceContext persistenceContext) {
		if ( LOG.isTraceEnabled() ) {
			final EntityEntry entityEntry = persistenceContext.getEntry( entity );
			LOG.tracef(
					"%s remove orphan before updates: [%s]",
					timing,
					entityEntry == null ? entityName : MessageHelper.infoString( entityName, entityEntry.getId() )
			);
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

	/**
	 * Convert a {@link LockMode} into a {@link LockOptions} object.
	 * <p>
	 *     We need to make sure that we use the method {@link LockOptions#setLockMode(LockMode)} for the conversion
	 *     because it also set a {@link LockOptions#timeout} that will affect the way SQL queries are generated.
	 *     There's also the constructor {@link LockOptions#LockOptions(LockMode)}, but it doesn't set a time-out
	 *     causing some generated SQL queries to not have the expected syntax (for example, it won't apply
	 *     the "nowait" clause in PostgreSQL, even if set to {@link LockMode#UPGRADE_NOWAIT} ).
	 * </p>
	 * @see <a href="https://github.com/hibernate/hibernate-reactive/issues/2534">Hibernate Reactive issue 2534</a>
	 */
	private static LockOptions toLockOptions(LockMode lockMode) {
		return lockMode == null
				? null
				: new LockOptions().setLockMode( lockMode );
	}
}

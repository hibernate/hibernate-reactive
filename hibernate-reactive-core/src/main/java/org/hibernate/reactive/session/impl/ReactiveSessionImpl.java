/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LazyInitializationException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.ObjectDeletedException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.TypeMismatchException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.ResultSetMappingDefinition;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.query.spi.QueryPlanCache;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryCollectionReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryConstructorReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryRootReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryScalarReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQuerySpecification;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.NamedQueryDefinition;
import org.hibernate.engine.spi.NamedSQLQueryDefinition;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.AutoFlushEvent;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.InitializeCollectionEvent;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.event.spi.LockEvent;
import org.hibernate.event.spi.MergeEvent;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.RefreshEvent;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionImpl;
import org.hibernate.internal.util.LockModeConverter;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.jpa.QueryHints;
import org.hibernate.jpa.spi.CriteriaQueryTupleTransformer;
import org.hibernate.jpa.spi.NativeQueryTupleTransformer;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.loader.custom.sql.SQLCustomQuery;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.Query;
import org.hibernate.query.internal.ParameterMetadataImpl;
import org.hibernate.reactive.common.ReactiveExceptionConverter;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.event.impl.DefaultReactiveAutoFlushEventListener;
import org.hibernate.reactive.event.impl.DefaultReactiveInitializeCollectionEventListener;
import org.hibernate.reactive.event.ReactiveDeleteEventListener;
import org.hibernate.reactive.event.ReactiveFlushEventListener;
import org.hibernate.reactive.event.ReactiveLoadEventListener;
import org.hibernate.reactive.event.ReactiveLockEventListener;
import org.hibernate.reactive.event.ReactiveMergeEventListener;
import org.hibernate.reactive.event.ReactivePersistEventListener;
import org.hibernate.reactive.event.ReactiveRefreshEventListener;
import org.hibernate.reactive.loader.custom.impl.ReactiveCustomLoader;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.CriteriaQueryOptions;
import org.hibernate.reactive.session.ReactiveNativeQuery;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.util.impl.CompletionStages;

import javax.persistence.EntityNotFoundException;
import javax.persistence.Tuple;
import javax.persistence.metamodel.Attribute;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An {@link ReactiveSession} implemented by extension of
 * the {@link SessionImpl} in Hibernate core. Extension was
 * preferred to delegation because there are places where
 * Hibernate core compares the identity of session instances.
 */
public class ReactiveSessionImpl extends SessionImpl implements ReactiveSession, EventSource {

	private transient ReactiveActionQueue reactiveActionQueue = new ReactiveActionQueue( this );
	private final ReactiveConnection reactiveConnection;

	//Lazily initialized
	private transient ExceptionConverter exceptionConverter;

	public ReactiveSessionImpl(SessionFactoryImpl delegate, SessionCreationOptions options,
							   ReactiveConnection connection) {
		super( delegate, options );
		reactiveConnection = connection;
	}

	@Override
	public SharedSessionContractImplementor getSharedContract() {
		return this;
	}

	@Override
	public Dialect getDialect() {
		return getJdbcServices().getDialect();
	}

	@Override
	protected StatefulPersistenceContext createPersistenceContext() {
		return new ReactivePersistenceContextAdapter( this );
	}

	@Override
	public ReactiveActionQueue getReactiveActionQueue() {
		return reactiveActionQueue;
	}

	@Override
	public Object immediateLoad(String entityName, Serializable id) throws HibernateException {
		throw new LazyInitializationException("reactive sessions do not support transparent lazy fetching"
				+ " - use Session.fetch() (entity '" + entityName + "' with id '" + id + "' was not loaded)");
	}

	@Override
	public <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy) {
		checkOpen();
		if ( association instanceof HibernateProxy ) {
			LazyInitializer initializer = ((HibernateProxy) association).getHibernateLazyInitializer();
			//TODO: is this correct?
			// SessionImpl doesn't use IdentifierLoadAccessImpl for initializing proxies
			String entityName = initializer.getEntityName();
			Serializable identifier = initializer.getIdentifier();
			return new ReactiveIdentifierLoadAccessImpl<T>( entityName )
					.fetch( identifier )
					.thenApply( SessionUtil.checkEntityFound( this, entityName, identifier ) )
					.thenApply( entity -> {
						initializer.setSession( this );
						initializer.setImplementation( entity );
						return unproxy ? entity : association;
					} );
		}
		else if ( association instanceof PersistentCollection ) {
			PersistentCollection persistentCollection = (PersistentCollection) association;
			return reactiveInitializeCollection( persistentCollection, false )
					.thenApply( pc -> association );
		}
		else {
			return CompletionStages.completedFuture( association );
		}
	}

	@Override
	public <E,T> CompletionStage<T> reactiveFetch(E entity, Attribute<E,T> field) {
		return ( (ReactiveEntityPersister) getEntityPersister( null, entity ) )
				.reactiveInitializeLazyProperty( field, entity, this );
	}

	@Override
	public <T> ReactiveNativeQueryImpl<T> createReactiveNativeQuery(String sqlString) {
		return getReactiveNativeQueryImplementor( sqlString, false );
	}

	@Override
	public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, String resultSetMapping) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			ReactiveNativeQuery<T> query = createReactiveNativeQuery( sqlString );
			query.setResultSetMapping( resultSetMapping );
			return query;
		}
		catch ( RuntimeException he ) {
			throw getExceptionConverter().convert( he );
		}
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		ResultSetMappingDefinition mapping = getFactory().getNamedQueryRepository()
				.getResultSetMappingDefinition( mappingName );
		if (mapping==null) {
			throw new IllegalArgumentException("result set mapping does not exist: " + mappingName);
		}

		if ( resultType!=null ) {
			Class<?> mappedResultType = getResultType(mapping);
			if ( !resultType.equals(mappedResultType) ) {
				throw new IllegalArgumentException("incorrect result type for result set mapping: "
						+ mappingName + " has type " + mappedResultType.getName() );
			}
		}

		return new ResultSetMapping<T>() {
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

	private Class<?> getResultType(ResultSetMappingDefinition mapping) {
		Class<?> mappedResultType = null;
		for ( NativeSQLQueryReturn queryReturn: mapping.getQueryReturns() ) {
			if (queryReturn instanceof NativeSQLQueryScalarReturn) {
				if (mappedResultType != null) {
					return Object[].class;
				}
				else {
					mappedResultType = ((NativeSQLQueryScalarReturn) queryReturn).getType().getReturnedClass();
				}
			}
			else if (queryReturn instanceof NativeSQLQueryRootReturn) {
				if (mappedResultType != null) {
					return Object[].class;
				}
				else {
					NativeSQLQueryRootReturn entityReturn = (NativeSQLQueryRootReturn) queryReturn;
					String entityName = entityReturn.getReturnEntityName();
					mappedResultType = getFactory().getMetamodel().entityPersister(entityName).getMappedClass();
				}
			}
			else if (queryReturn instanceof NativeSQLQueryCollectionReturn) {
				if (mappedResultType != null) {
					return Object[].class;
				}
				else {
					NativeSQLQueryCollectionReturn collectionReturn = (NativeSQLQueryCollectionReturn) queryReturn;
					String entityName = collectionReturn.getOwnerEntityName();
					String propertyName = collectionReturn.getOwnerProperty();
					String role = entityName + '.' + propertyName;
					mappedResultType = getFactory().getMetamodel().collectionPersister(role).getElementClass();
				}
			}
			else if (queryReturn instanceof NativeSQLQueryConstructorReturn) {
				if (mappedResultType != null) {
					return Object[].class;
				}
				else {
					mappedResultType = ((NativeSQLQueryConstructorReturn) queryReturn).getTargetClass();
				}
			}
		}
		return mappedResultType;
	}

	@Override
	public <T> ReactiveQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultClass) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			ReactiveNativeQuery<T> query = createReactiveNativeQuery( sqlString );
			handleNativeQueryResult( query, resultClass );
			return query;
		}
		catch ( RuntimeException he ) {
			throw getExceptionConverter().convert( he );
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

	private <T> ReactiveNativeQueryImpl<T> getReactiveNativeQueryImplementor(
			String queryString,
			boolean isOrdinalParameterZeroBased) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			ParameterMetadata params = getFactory().getQueryPlanCache()
					.getSQLParameterMetadata( queryString, isOrdinalParameterZeroBased );
			ReactiveNativeQueryImpl<T> query = new ReactiveNativeQueryImpl<>( queryString, false, this, params );
			query.setComment( "dynamic native SQL query" );
			applyQuerySettingsAndHints( query );
			return query;
		}
		catch ( RuntimeException he ) {
			throw getExceptionConverter().convert( he );
		}
	}

	private static QueryType queryType(String queryString) {
		queryString = queryString.trim().toLowerCase();
		return queryString.startsWith("insert")
			|| queryString.startsWith("update")
			|| queryString.startsWith("delete")
				? QueryType.INSERT_UPDATE_DELETE
				: QueryType.SELECT;
	}

	@Override
	public <R> ReactiveQueryImpl<R> createReactiveQuery(String queryString) {
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			ParameterMetadataImpl paramMetadata = getQueryPlan( queryString, false ).getParameterMetadata();
			ReactiveQueryImpl<R> query = new ReactiveQueryImpl<>( this, paramMetadata, queryString, queryType(queryString) );
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
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			// do the translation
			final ReactiveQueryImpl<R> query = createReactiveQuery( queryString );
			resultClassChecking( resultType, query.unwrap( Query.class ) );
			return query;
		}
		catch (RuntimeException e) {
			throw getExceptionConverter().convert( e );
		}
	}

	/**
	 * @deprecated use {@link #reactiveInitializeCollection(PersistentCollection, boolean)} instead
	 */
	@Deprecated
	@Override
	public void initializeCollection(PersistentCollection collection, boolean writing) {
		throw getExceptionConverter().convert( new UnsupportedOperationException( "ReactiveSessionImpl#initializeCollection not supported, use reactiveInitializeCollection instead" ) );
	}

	public CompletionStage<Void> reactiveInitializeCollection(PersistentCollection collection, boolean writing) {
		checkOpenOrWaitingForAutoClose();
		pulseTransactionCoordinator();
		InitializeCollectionEvent event = new InitializeCollectionEvent( collection, this );
		return fire( event, EventType.INIT_COLLECTION,
				(DefaultReactiveInitializeCollectionEventListener l) -> l::onReactiveInitializeCollection )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				} );
	}

	@Override
	protected ReactiveHQLQueryPlan getQueryPlan(String query, boolean shallow) throws HibernateException {
		QueryPlanCache queryPlanCache = getFactory().getQueryPlanCache();
		return (ReactiveHQLQueryPlan) queryPlanCache.getHQLQueryPlan( query, shallow, getLoadQueryInfluencers().getEnabledFilters() );
	}

	//TODO: parameterize the SessionFactory constructor by ReactiveNativeSQLQueryPlan::new
//	@Override
//	protected ReactiveNativeSQLQueryPlan getNativeQueryPlan(NativeSQLQuerySpecification spec) throws HibernateException {
//		QueryPlanCache queryPlanCache = getFactory().getQueryPlanCache();
//		return (ReactiveNativeSQLQueryPlan) queryPlanCache.getNativeSQLQueryPlan( spec );
//	}

	protected CompletionStage<Void> reactiveAutoFlushIfRequired(Set<?> querySpaces) throws HibernateException {
		checkOpen();
//		if ( !isTransactionInProgress() ) {
			// do not auto-flush while outside a transaction
//			return CompletionStages.nullFuture();
//		}
		AutoFlushEvent event = new AutoFlushEvent( querySpaces, this );
		return fire( event, EventType.AUTO_FLUSH, (DefaultReactiveAutoFlushEventListener l) -> l::reactiveOnAutoFlush );
	}

	@Override
	public <T> CompletionStage<List<T>> reactiveList(String query, QueryParameters parameters) throws HibernateException {
		checkOpenOrWaitingForAutoClose();
		pulseTransactionCoordinator();
		parameters.validateParameters();

		HQLQueryPlan plan = parameters.getQueryPlan();
		if ( plan == null ) {
			plan = getQueryPlan( query, false );
		}
		ReactiveHQLQueryPlan reactivePlan = (ReactiveHQLQueryPlan) plan;

		return reactiveAutoFlushIfRequired( plan.getQuerySpaces() )
				// FIXME: I guess I can fix this as a separate issue
//				dontFlushFromFind++;   //stops flush being called multiple times if this method is recursively called
				.thenCompose( v -> reactivePlan.performReactiveList(parameters, this ) )
				.whenComplete( (list, x) -> {
//					dontFlushFromFind--;
					afterOperation( x == null );
					delayedAfterCompletion();
				} )
				//TODO: this typecast is rubbish
				.thenApply( list -> (List<T>) list );
	}

	@Override
	public <T> CompletionStage<List<T>> reactiveList(NativeSQLQuerySpecification spec, QueryParameters parameters) {
		return listReactiveCustomQuery( getNativeQueryPlan( spec ).getCustomQuery(), parameters)
				//TODO: this typecast is rubbish
				.thenApply( list -> (List<T>) list );
	}

	private CompletionStage<List<Object>> listReactiveCustomQuery(CustomQuery customQuery, QueryParameters parameters) {
		checkOpenOrWaitingForAutoClose();
//		checkTransactionSynchStatus();

		ReactiveCustomLoader loader = new ReactiveCustomLoader( customQuery, getFactory() );

//		autoFlushIfRequired( loader.getQuerySpaces() );

//		dontFlushFromFind++;
//		boolean success = false;
			return loader.reactiveList( this, parameters )
					.whenComplete( (r, e) -> delayedAfterCompletion() );
//			success = true;
//			dontFlushFromFind--;
//			afterOperation( success );
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveNamedQuery(String name) {
		return buildReactiveQueryFromName( name, null );
	}

	@Override
	public <R> ReactiveQuery<R> createReactiveNamedQuery(String name, Class<R> resultClass) {
		return buildReactiveQueryFromName( name, resultClass );
	}

	private <T> ReactiveQuery<T> buildReactiveQueryFromName(String name, Class<T> resultType) {
		checkOpen();
		try {
			pulseTransactionCoordinator();
			delayedAfterCompletion();

			// todo : apply stored setting at the JPA Query level too

			final NamedQueryDefinition namedQueryDefinition = getFactory().getNamedQueryRepository().getNamedQueryDefinition( name );
			if ( namedQueryDefinition != null ) {
				return createReactiveQuery( namedQueryDefinition, resultType );
			}

			final NamedSQLQueryDefinition nativeQueryDefinition = getFactory().getNamedQueryRepository().getNamedSQLQueryDefinition( name );
			if ( nativeQueryDefinition != null ) {
				return createReactiveNativeQuery( nativeQueryDefinition, resultType );
			}

			throw getExceptionConverter().convert( new IllegalArgumentException( "No query defined for that name [" + name + "]" ) );
		}
		catch (RuntimeException e) {
			throw !( e instanceof IllegalArgumentException ) ? new IllegalArgumentException( e ) : e;
		}
	}

	private <T> ReactiveQuery<T> createReactiveQuery(NamedQueryDefinition namedQueryDefinition, Class<T> resultType) {
		final ReactiveQuery<T> query = createReactiveQuery( namedQueryDefinition );
		if ( resultType != null ) {
			resultClassChecking( resultType, createQuery( namedQueryDefinition ) );
		}
		return query;
	}

	private <T> ReactiveQuery<T> createReactiveQuery(NamedQueryDefinition queryDefinition) {
		String queryString = queryDefinition.getQueryString();
		ParameterMetadataImpl paramMetadata = getQueryPlan( queryString, false ).getParameterMetadata();
		ReactiveQueryImpl<T> query = new ReactiveQueryImpl<>( this, paramMetadata, queryString, queryType( queryString ) );
		applyQuerySettingsAndHints( query );
		query.setHibernateFlushMode( queryDefinition.getFlushMode() );
		query.setComment( queryDefinition.getComment() != null ? queryDefinition.getComment() : queryDefinition.getName() );
		if ( queryDefinition.getLockOptions() != null ) {
			query.setLockOptions( queryDefinition.getLockOptions() );
		}

		initQueryFromNamedDefinition( query, queryDefinition );

		return query;
	}

	private <T> ReactiveNativeQuery<T> createReactiveNativeQuery(NamedSQLQueryDefinition queryDefinition, Class<T> resultType) {
		if ( resultType != null && !Tuple.class.equals( resultType ) && !Object[].class.equals( resultType ) ) {
			resultClassChecking( resultType, queryDefinition );
		}

		final ReactiveNativeQueryImpl<T> query = new ReactiveNativeQueryImpl<>(
				queryDefinition,
				this,
				getFactory().getQueryPlanCache().getSQLParameterMetadata( queryDefinition.getQueryString(), false )
		);
		if ( Tuple.class.equals( resultType ) ) {
			query.setResultTransformer( new NativeQueryTupleTransformer() );
		}
		applyQuerySettingsAndHints( query );
		query.setHibernateFlushMode( queryDefinition.getFlushMode() );
		query.setComment( queryDefinition.getComment() != null ? queryDefinition.getComment() : queryDefinition.getName() );
		if ( queryDefinition.getLockOptions() != null ) {
			query.setLockOptions( queryDefinition.getLockOptions() );
		}

		initQueryFromNamedDefinition( query, queryDefinition );

		return query;
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
	public CompletionStage<Integer> executeReactiveUpdate(String query, QueryParameters parameters) {
		checkOpenOrWaitingForAutoClose();
		pulseTransactionCoordinator();
		parameters.validateParameters();

		ReactiveHQLQueryPlan reactivePlan = getQueryPlan( query, false );
		return reactiveAutoFlushIfRequired( reactivePlan.getQuerySpaces() )
				.thenAccept( v -> verifyImmutableEntityUpdate( reactivePlan ) )
				.thenCompose( v -> reactivePlan.performExecuteReactiveUpdate( parameters, this ) )
				.whenComplete( (count, x) -> {
					afterOperation( x == null );
					delayedAfterCompletion();
				} );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(NativeSQLQuerySpecification specification,
														  QueryParameters parameters) {
		checkOpenOrWaitingForAutoClose();
		pulseTransactionCoordinator();
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
		return reactiveAutoFlushIfRequired( reactivePlan.getCustomQuery().getQuerySpaces() )
				.thenCompose( v -> reactivePlan.performExecuteReactiveUpdate( parameters, this ) )
				.whenComplete( (count, x) -> {
					afterOperation( x == null );
					delayedAfterCompletion();
				} );
	}

	@Override
	public CompletionStage<Void> reactivePersist(Object entity) {
		checkOpen();
		return firePersist( new PersistEvent( null, entity, this ) );
	}

	@Override
	public CompletionStage<Void> reactivePersist(Object object, IdentitySet copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersist( copiedAlready, new PersistEvent( null, object, this ) );
	}

	// Should be similar to firePersist
	private CompletionStage<Void> firePersist(PersistEvent event) {
		checkTransactionSynchStatus();
		checkNoUnresolvedActionsBeforeOperation();

		return fire(event, EventType.PERSIST, (ReactivePersistEventListener l) -> l::reactiveOnPersist)
				.handle( (v, e) -> {
					checkNoUnresolvedActionsAfterOperation();

					if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	private CompletionStage<Void> firePersist(IdentitySet copiedAlready, PersistEvent event) {
		pulseTransactionCoordinator();

		return fire(event, copiedAlready, EventType.PERSIST,
				(ReactivePersistEventListener l) -> l::reactiveOnPersist)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	@Override
	public CompletionStage<Void> reactivePersistOnFlush(Object entity, IdentitySet copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersistOnFlush( copiedAlready, new PersistEvent( null, entity, this ) );
	}

	private CompletionStage<Void> firePersistOnFlush(IdentitySet copiedAlready, PersistEvent event) {
		pulseTransactionCoordinator();

		return fire(event, copiedAlready, EventType.PERSIST_ONFLUSH,
				(ReactivePersistEventListener l) -> l::reactiveOnPersist)
				.whenComplete( (v, e) -> delayedAfterCompletion() );
	}

	@Override
	public CompletionStage<Void> reactiveRemove(Object entity) {
		checkOpen();
		return fireRemove( new DeleteEvent( null, entity, this ) );
	}

	@Override
	public CompletionStage<Void> reactiveRemove(Object object, boolean isCascadeDeleteEnabled, IdentitySet transientEntities)
			throws HibernateException {
		checkOpenOrWaitingForAutoClose();
		return fireRemove(
				new DeleteEvent(
						null,
						object,
						isCascadeDeleteEnabled,
						((ReactivePersistenceContextAdapter) getPersistenceContextInternal())
								.isRemovingOrphanBeforeUpates(),
						this
				),
				transientEntities
		);
	}

	// Should be similar to fireRemove
	private CompletionStage<Void> fireRemove(DeleteEvent event) {
		pulseTransactionCoordinator();

		return fire(event, EventType.DELETE,
				(ReactiveDeleteEventListener l) -> l::reactiveOnDelete)
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
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	private CompletionStage<Void> fireRemove(DeleteEvent event, IdentitySet transientEntities) {
		pulseTransactionCoordinator();

		return fire(event, transientEntities, EventType.DELETE,
				(ReactiveDeleteEventListener l) -> l::reactiveOnDelete)
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
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	@Override
	public <T> CompletionStage<T> reactiveMerge(T object) throws HibernateException {
		checkOpen();
		return fireMerge( new MergeEvent( null, object, this ));
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

		return fire(event, EventType.MERGE,
				(ReactiveMergeEventListener l) -> l::reactiveOnMerge)
				.handle( (v,e) -> {
					checkNoUnresolvedActionsAfterOperation();

					if (e instanceof ObjectDeletedException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if (e instanceof RuntimeException) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnOrRethrow( e, (T) event.getResult() );
				});
	}

	private CompletionStage<Void> fireMerge(MergeContext copiedAlready, MergeEvent event) {
		pulseTransactionCoordinator();

		return fire(event, copiedAlready, EventType.MERGE,
				(ReactiveMergeEventListener l) -> l::reactiveOnMerge)
				.handle( (v,e) -> {
					delayedAfterCompletion();

					if (e instanceof ObjectDeletedException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if (e instanceof RuntimeException) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				});

	}

	@Override
	public CompletionStage<Void> reactiveFlush() {
		checkOpen();
		return doFlush();
	}

	@Override
	public CompletionStage<Void> reactiveAutoflush() {
		return getHibernateFlushMode().lessThan( FlushMode.COMMIT )
				? CompletionStages.nullFuture()
				: doFlush();
	}

	private CompletionStage<Void> doFlush() {
		checkTransactionNeededForUpdateOperation( "no transaction is in progress" );
		pulseTransactionCoordinator();

		if ( getPersistenceContextInternal().getCascadeLevel() > 0 ) {
			throw new HibernateException( "Flush during cascade is dangerous" );
		}

		return fire(new FlushEvent( this ), EventType.FLUSH,
				(ReactiveFlushEventListener l) -> l::reactiveOnFlush)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof CompletionException ) {
						if ( e.getCause() instanceof RuntimeException ) {
							e = getExceptionConverter().convert( (RuntimeException) e.getCause() );
						}
					}
					return CompletionStages.returnNullorRethrow( e );
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
	public CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode) {
		checkOpen();
		return fireRefresh( new RefreshEvent( entity, lockMode, this ) );
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object object, IdentitySet refreshedAlready) {
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

		return fire(event, EventType.REFRESH,
				(ReactiveRefreshEventListener l) -> l::reactiveOnRefresh)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof RuntimeException) {
						if ( !getSessionFactory().getSessionFactoryOptions().isJpaBootstrap() ) {
							if ( e instanceof HibernateException ) {
								return CompletionStages.rethrow(e);
							}
						}
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	private CompletionStage<Void> fireRefresh(IdentitySet refreshedAlready, RefreshEvent event) {
		pulseTransactionCoordinator();

		return fire(event, refreshedAlready, EventType.REFRESH,
				(ReactiveRefreshEventListener l) -> l::reactiveOnRefresh)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	@Override
	public CompletionStage<Void> reactiveLock(Object object, LockMode lockMode) {
		checkOpen();
		return fireLock( new LockEvent( object, lockMode, this ) );
	}

	private CompletionStage<Void> fireLock(LockEvent event) {
		pulseTransactionCoordinator();

		return fire( event, EventType.LOCK, (ReactiveLockEventListener l) -> l::reactiveOnLock )
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return CompletionStages.returnNullorRethrow( e );
				});
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(
			Class<T> entityClass,
			Serializable id) {
		return new ReactiveIdentifierLoadAccessImpl<>( entityClass ).load( id );
	}

	@Override
	public <T> CompletionStage<T> reactiveFind(
			Class<T> entityClass,
			Object id,
			LockMode lockMode,
			Map<String, Object> properties) {
		checkOpen();

		getLoadQueryInfluencers().getEffectiveEntityGraph().applyConfiguredGraph( properties );

		Boolean readOnly = properties == null ? null : (Boolean) properties.get( QueryHints.HINT_READONLY );
		getLoadQueryInfluencers().setReadOnly( readOnly );

		final ReactiveIdentifierLoadAccessImpl<T> loadAccess =
				new ReactiveIdentifierLoadAccessImpl<>(entityClass)
						.with( determineAppropriateLocalCacheMode( properties ) );

		LockOptions lockOptions;
		if ( lockMode != null ) {
//			if ( !LockModeType.NONE.equals( lockModeType) ) {
//					checkTransactionNeededForUpdateOperation();
//			}
			lockOptions = buildLockOptions(
					LockModeConverter.convertToLockModeType(lockMode),
					properties
			);
			loadAccess.with( lockOptions );
		}
		else {
			lockOptions = null;
		}

		return loadAccess.load( (Serializable) id )
				.handle( (result, e) -> {
					if ( e instanceof EntityNotFoundException) {
						// DefaultLoadEventListener.returnNarrowedProxy may throw ENFE (see HHH-7861 for details),
						// which find() should not throw. Find() should return null if the entity was not found.
						//			if ( log.isDebugEnabled() ) {
						//				String entityName = entityClass != null ? entityClass.getName(): null;
						//				String identifierValue = id != null ? id.toString() : null ;
						//				log.ignoringEntityNotFound( entityName, identifierValue );
						//			}
						return null;
					}
					if ( e instanceof ObjectDeletedException) {
						//the spec is silent about people doing remove() find() on the same PC
						return null;
					}
					if ( e instanceof ObjectNotFoundException) {
						//should not happen on the entity itself with get
						throw new IllegalArgumentException( e.getMessage(), e );
					}
					if ( e instanceof MappingException
							|| e instanceof TypeMismatchException
							|| e instanceof ClassCastException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					if ( e instanceof JDBCException ) {
//						if ( accessTransaction().getRollbackOnly() ) {
//							// assume this is the similar to the WildFly / IronJacamar "feature" described under HHH-12472
//							return null;
//						}
						throw getExceptionConverter().convert( (JDBCException) e, lockOptions );
					}
					if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e, lockOptions );
					}

					return result;
				} )
				.whenComplete( (v, e) -> getLoadQueryInfluencers().getEffectiveEntityGraph().clear() );
	}

	@Override
	public <T> CompletionStage<List<T>> reactiveFind(Class<T> entityClass, Object... ids) {
		return new ReactiveMultiIdentifierLoadAccessImpl<>(entityClass).multiLoad(ids);
		//TODO: copy/paste the exception handling from immediately above?
	}

	@SuppressWarnings("unchecked")
	private <E, L, RL, T> CompletionStage<T> fire(
			E event,
			EventType<L> eventType,
			Function<RL, Function<E, CompletionStage<T>>> fun) {
		CompletionStage<T> ret = CompletionStages.nullFuture();
		for ( L listener : eventListeners( eventType ) ) {
			//to preserve atomicity of the Session methods
			//call apply() from within the arg of thenCompose()
			ret = ret.thenCompose( v -> fun.apply((RL) listener).apply(event) );
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	private <E,L,RL,P> CompletionStage<Void> fire(E event, P extra, EventType<L> eventType,
												  Function<RL, BiFunction<E, P, CompletionStage<Void>>> fun) {
		CompletionStage<Void> ret = CompletionStages.nullFuture();
		for ( L listener : eventListeners(eventType) ) {
			//to preserve atomicity of the Session methods
			//call apply() from within the arg of thenCompose()
			ret = ret.thenCompose( v -> fun.apply((RL) listener).apply(event, extra) );
		}
		return ret;
	}

	@SuppressWarnings("deprecation")
	private <T> Iterable<T> eventListeners(EventType<T> type) {
		return getFactory().unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry().getService( EventListenerRegistry.class )
				.getEventListenerGroup( type )
				.listeners();
	}

	private CompletionStage<Void> fireLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
		checkOpenOrWaitingForAutoClose();

		return fireLoadNoChecks( event, loadType )
				.whenComplete( (v, e) -> delayedAfterCompletion() );
	}

	private CompletionStage<Void> fireLoadNoChecks(LoadEvent event, LoadEventListener.LoadType loadType) {
		pulseTransactionCoordinator();

		return fire(event, loadType, EventType.LOAD, (ReactiveLoadEventListener l) -> l::reactiveOnLoad);
	}

	@Override
	protected void delayedAfterCompletion() {
		//disable for now, but figure out what to do here
	}

	public void afterOperation(boolean success) {
		//disable for now, but figure out what to do here
	}

	@Override
	public void checkTransactionNeededForUpdateOperation(String exceptionMessage) {
		//no-op because we don't support transactions
	}

	private class ReactiveIdentifierLoadAccessImpl<T> {

		private final EntityPersister entityPersister;

		private LockOptions lockOptions;
		private CacheMode cacheMode;

		//Note that entity graphs aren't supported at all
		//because we're not using the EntityLoader from
		//the plan package, so this stuff is useless
		private RootGraphImplementor<T> rootGraph;
		private GraphSemantic graphSemantic;

		public ReactiveIdentifierLoadAccessImpl(EntityPersister entityPersister) {
			this.entityPersister = entityPersister;
		}

		public ReactiveIdentifierLoadAccessImpl(String entityName) {
			this( getFactory().getMetamodel().locateEntityPersister( entityName ) );
		}

		public ReactiveIdentifierLoadAccessImpl(Class<T> entityClass) {
			this( getFactory().getMetamodel().locateEntityPersister( entityClass ) );
		}

		public final ReactiveIdentifierLoadAccessImpl<T> with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		public ReactiveIdentifierLoadAccessImpl<T> with(CacheMode cacheMode) {
			this.cacheMode = cacheMode;
			return this;
		}

		public ReactiveIdentifierLoadAccessImpl<T> with(RootGraph<T> graph, GraphSemantic semantic) {
			rootGraph = (RootGraphImplementor<T>) graph;
			graphSemantic = semantic;
			return this;
		}

		public final CompletionStage<T> getReference(Serializable id) {
			return perform( () -> doGetReference( id ) );
		}

		protected CompletionStage<T> perform(Supplier<CompletionStage<T>> executor) {
			if ( graphSemantic != null ) {
				if ( rootGraph == null ) {
					throw new IllegalArgumentException( "Graph semantic specified, but no RootGraph was supplied" );
				}
			}
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

		@SuppressWarnings("unchecked")
		protected CompletionStage<T> doGetReference(Serializable id) {
			if ( lockOptions != null ) {
				LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), lockOptions, ReactiveSessionImpl.this, getReadOnlyFromLoadQueryInfluencers());
				return fireLoad( event, LoadEventListener.LOAD ).thenApply( v -> (T) event.getResult() );
			}

			LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), false, ReactiveSessionImpl.this, getReadOnlyFromLoadQueryInfluencers());
			return fireLoad( event, LoadEventListener.LOAD )
					.thenApply( v -> {
						if ( event.getResult() == null ) {
							getFactory().getEntityNotFoundDelegate().handleEntityNotFound(
									entityPersister.getEntityName(),
									id
							);
						}
						return (T) event.getResult();
					} ).whenComplete( (v, x) -> afterOperation( x != null ) );
		}

		public final CompletionStage<T> load(Serializable id) {
			return perform( () -> doLoad( id, LoadEventListener.GET) );
		}

		public final CompletionStage<T> fetch(Serializable id) {
			return perform( () -> doLoad( id, LoadEventListener.IMMEDIATE_LOAD) );
		}

		private Boolean getReadOnlyFromLoadQueryInfluencers() {
			return getLoadQueryInfluencers().getReadOnly();
		}

		@SuppressWarnings("unchecked")
		protected final CompletionStage<T> doLoad(Serializable id, LoadEventListener.LoadType loadType) {
			if ( lockOptions != null ) {
				LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), lockOptions, ReactiveSessionImpl.this, getReadOnlyFromLoadQueryInfluencers());
				return fireLoad( event, loadType ).thenApply( v -> (T) event.getResult() );
			}

			LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), false, ReactiveSessionImpl.this, getReadOnlyFromLoadQueryInfluencers());
			return fireLoad( event, loadType )
					.whenComplete( (v, t) -> afterOperation( t != null ) )
					.thenApply( v -> (T) event.getResult() );
		}
	}

	private class ReactiveMultiIdentifierLoadAccessImpl<T> implements MultiLoadOptions {
		private final EntityPersister entityPersister;

		private LockOptions lockOptions;
		private CacheMode cacheMode;

		private RootGraphImplementor<T> rootGraph;
		private GraphSemantic graphSemantic;

		private Integer batchSize;
		private boolean sessionCheckingEnabled;
		private boolean returnOfDeletedEntitiesEnabled;
		private boolean orderedReturnEnabled = true;

		public ReactiveMultiIdentifierLoadAccessImpl(EntityPersister entityPersister) {
			this.entityPersister = entityPersister;
		}

		public ReactiveMultiIdentifierLoadAccessImpl(Class<T> entityClass) {
			this( getFactory().getMetamodel().locateEntityPersister( entityClass ) );
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
			if ( batchSize < 1 ) {
				this.batchSize = null;
			}
			else {
				this.batchSize = batchSize;
			}
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

		@SuppressWarnings("unchecked")
		public CompletionStage<List<T>> multiLoad(Object... ids) {
			Serializable[] sids = new Serializable[ids.length];
			System.arraycopy(ids, 0, sids, 0, ids.length);
			return perform( () -> (CompletionStage)
					((ReactiveEntityPersister) entityPersister).reactiveMultiLoad( sids, ReactiveSessionImpl.this, this ) );
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

		@SuppressWarnings("unchecked")
		public <K extends Serializable> CompletionStage<List<T>> multiLoad(List<K> ids) {
			return perform( () -> (CompletionStage<List<T>>)
					entityPersister.multiLoad( ids.toArray(new Serializable[0]), ReactiveSessionImpl.this, this ) );
		}
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		if ( ReactiveSession.class.isAssignableFrom( clazz ) ) {
			return clazz.cast(this);
		}
		if ( Stage.Session.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( new StageSessionImpl( this ) );
		}
		if ( Mutiny.Session.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( new MutinySessionImpl( this ) );
		}
		return super.unwrap( clazz );
	}

	public ReactiveConnection getReactiveConnection() {
		return reactiveConnection;
	}

	@Override
	public void close() throws HibernateException {
		if ( reactiveConnection != null ) {
			reactiveConnection.close();
		}
		super.close();
	}

	@Override @SuppressWarnings("unchecked")
	public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity, String name) {
		RootGraphImplementor<?> entityGraph = super.createEntityGraph(name);
		if ( !entityGraph.getGraphedType().getJavaType().equals(entity) ) {
			throw new HibernateException("wrong entity type");
		}
		return (RootGraphImplementor<T>) entityGraph;
	}

	public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity) {
		return super.createEntityGraph( entity );
	}

	@Override @SuppressWarnings("unchecked")
	public <T> RootGraphImplementor<T> getEntityGraph(Class<T> entity, String name) {
		RootGraphImplementor<?> entityGraph = super.getEntityGraph(name);
		if ( !entityGraph.getGraphedType().getJavaType().equals(entity) ) {
			throw new HibernateException("wrong entity type");
		}
		return (RootGraphImplementor<T>) entityGraph;
	}
}

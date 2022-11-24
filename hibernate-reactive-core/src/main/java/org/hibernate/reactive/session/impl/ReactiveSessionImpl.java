/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.ObjectDeletedException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.TypeMismatchException;
import org.hibernate.UnresolvableObjectException;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.internal.StatefulPersistenceContext;
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
import org.hibernate.event.spi.ResolveNaturalIdEvent;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionImpl;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.query.criteria.JpaCriteriaInsertSelect;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.reactive.common.InternalStateAssertions;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.event.ReactiveDeleteEventListener;
import org.hibernate.reactive.event.ReactiveFlushEventListener;
import org.hibernate.reactive.event.ReactiveLoadEventListener;
import org.hibernate.reactive.event.ReactiveLockEventListener;
import org.hibernate.reactive.event.ReactiveMergeEventListener;
import org.hibernate.reactive.event.ReactivePersistEventListener;
import org.hibernate.reactive.event.ReactiveRefreshEventListener;
import org.hibernate.reactive.event.ReactiveResolveNaturalIdEventListener;
import org.hibernate.reactive.event.impl.DefaultReactiveAutoFlushEventListener;
import org.hibernate.reactive.event.impl.DefaultReactiveInitializeCollectionEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.query.ReactiveMutationQuery;
import org.hibernate.reactive.query.ReactiveNativeQuery;
import org.hibernate.reactive.query.ReactiveQuery;
import org.hibernate.reactive.query.ReactiveSelectionQuery;
import org.hibernate.reactive.query.sql.internal.ReactiveNativeQueryImpl;
import org.hibernate.reactive.query.sqm.iternal.ReactiveQuerySqmImpl;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.engine.spi.NaturalIdResolutions.INVALID_NATURAL_ID_REFERENCE;
import static org.hibernate.reactive.common.InternalStateAssertions.assertUseOnEventLoop;
import static org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister.forceInitialize;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.returnNullorRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * An {@link ReactiveSession} implemented by extension of
 * the {@link SessionImpl} in Hibernate core. Extension was
 * preferred to delegation because there are places where
 * Hibernate core compares the identity of session instances.
 */
public class ReactiveSessionImpl extends SessionImpl implements ReactiveSession, EventSource {
	private static final Log log = LoggerFactory.make( Log.class, MethodHandles.lookup() );

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
	protected StatefulPersistenceContext createPersistenceContext() {
		return new ReactivePersistenceContextAdapter( this );
	}

	@Override
	public ReactiveActionQueue getReactiveActionQueue() {
		threadCheck();
		return reactiveActionQueue;
	}

	@Override
	public Object immediateLoad(String entityName, Object id) throws HibernateException {
		throw log.lazyInitializationException( entityName, id );
	}

	/**
	 * Load the data for the object with the specified id into a newly created object.
	 * This is only called when lazily initializing a proxy. Do NOT return a proxy.
	 */
	@Override
	public CompletionStage<Object> reactiveImmediateLoad(String entityName, Object id)
			throws HibernateException {
//		if ( log.isDebugEnabled() ) {
//			EntityPersister persister = getFactory().getMetamodel().entityPersister( entityName );
//			log.debugf( "Initializing proxy: %s", MessageHelper.infoString( persister, id, getFactory() ) );
//		}
		threadCheck();
		LoadEvent event = new LoadEvent(
				id, entityName, true, this,
				getReadOnlyFromLoadQueryInfluencers()
		);
		return fireLoadNoChecks( event, LoadEventListener.IMMEDIATE_LOAD )
				.thenApply( v -> event.getResult() );
	}

	@Override
	public CompletionStage<Object> reactiveInternalLoad(String entityName, Object id, boolean eager, boolean nullable) {
		final EffectiveEntityGraph effectiveEntityGraph = getLoadQueryInfluencers().getEffectiveEntityGraph();
		final GraphSemantic semantic = effectiveEntityGraph.getSemantic();
		final RootGraphImplementor<?> graph = effectiveEntityGraph.getGraph();
		boolean clearedEffectiveGraph;
		if ( semantic == null || graph.appliesTo( entityName ) ) {
			clearedEffectiveGraph = false;
		}
		else {
//				log.debug( "Clearing effective entity graph for subsequent-select" );
			clearedEffectiveGraph = true;
			effectiveEntityGraph.clear();
		}

		final LoadEventListener.LoadType type;
		if ( nullable ) {
			type = LoadEventListener.INTERNAL_LOAD_NULLABLE;
		}
		else {
			type = eager
					? LoadEventListener.INTERNAL_LOAD_EAGER
					: LoadEventListener.INTERNAL_LOAD_LAZY;
		}

		threadCheck();
		LoadEvent event =
				new LoadEvent( id, entityName, true, this, getReadOnlyFromLoadQueryInfluencers() );
		return fireLoadNoChecks( event, type )
				.thenApply( v -> {
					Object result = event.getResult();
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
	//Note: when making changes to this method, please also consider
	//      the similar code in Mutiny.fetch() and Stage.fetch()
	public <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy) {
		checkOpen();
		if ( association == null ) {
			return nullFuture();
		}

		if ( association instanceof HibernateProxy ) {
			LazyInitializer initializer = ( (HibernateProxy) association ).getHibernateLazyInitializer();
			if ( !initializer.isUninitialized() ) {
				return completedFuture( unproxy ? (T) initializer.getImplementation() : association );
			}
			else {
				String entityName = initializer.getEntityName();
				Object identifier = initializer.getIdentifier();
				return reactiveImmediateLoad( entityName, identifier )
						.thenApply( entity -> {
							checkEntityFound( this, entityName, identifier, entity );
							initializer.setSession( this );
							initializer.setImplementation( entity );
							return unproxy ? (T) entity : association;
						} );
			}
		}
		else if ( association instanceof PersistentCollection ) {
			PersistentCollection persistentCollection = (PersistentCollection) association;
			if ( persistentCollection.wasInitialized() ) {
				return completedFuture( association );
			}
			else {
				return reactiveInitializeCollection( persistentCollection, false )
						// don't reassociate the collection instance, because
						// its owner isn't associated with this session
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
				return completedFuture( association );
			}
		}
		else {
			return completedFuture( association );
		}
	}

	@Override
	public <E, T> CompletionStage<T> reactiveFetch(E entity, Attribute<E, T> field) {
		return ( (ReactiveEntityPersister) getEntityPersister( null, entity ) )
				.reactiveInitializeLazyProperty( field, entity, this );
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
	public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString) {
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
	public <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultType) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, Class<R> resultClass, String tableAlias) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, String resultSetMappingName, Class<R> resultClass) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString, Class<R> resultType) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(CriteriaQuery<R> criteria) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(String hqlString) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaUpdate updateQuery) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(JpaCriteriaInsertSelect insertSelect) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveMutationQuery<R> createNamedReactiveMutationQuery(String name, Class<R> resultClass) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String name) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveMutationQuery<R> createNamedReactiveMutationQuery(String name) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveMutationQuery<R> createNativeReactiveMutationQuery(String sqlString) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveQuery getNamedReactiveQuery(String queryName) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveNativeQuery getNamedReactiveNativeQuery(String name) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public ReactiveNativeQuery getNamedReactiveNativeQuery(String name, String resultSetMapping) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String name, Class<R> resultType) {
		throw new NotYetImplementedFor6Exception();
	}

	/**
	 * @deprecated use {@link #reactiveInitializeCollection(PersistentCollection, boolean)} instead
	 */
	@Deprecated
	@Override
	public void initializeCollection(PersistentCollection collection, boolean writing) {
		throw log.collectionCannotBeInitializedlazyInitializationException( collectionRoleLogMessage( collection ) );
	}

	private static String collectionRoleLogMessage(PersistentCollection collection) {
		if ( collection == null ) {
			return "collection is null";
		}
		return collection.getRole();
	}

	@Override
	public CompletionStage<Void> reactiveInitializeCollection(PersistentCollection collection, boolean writing) {
		checkOpenOrWaitingForAutoClose();
		pulseTransactionCoordinator();
		InitializeCollectionEvent event = new InitializeCollectionEvent( collection, this );

		return fastSessionServices.eventListenerGroup_INIT_COLLECTION
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
	public CompletionStage<Void> reactivePersist(Object object, PersistContext copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersist( copiedAlready, new PersistEvent( null, object, this ) );
	}

	// Should be similar to firePersist
	private CompletionStage<Void> firePersist(PersistEvent event) {
		checkTransactionSynchStatus();
		checkNoUnresolvedActionsBeforeOperation();

		return fastSessionServices.eventListenerGroup_PERSIST
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

		return fastSessionServices.eventListenerGroup_PERSIST
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

		return fastSessionServices.eventListenerGroup_PERSIST
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
			boolean isCascadeDeleteEnabled,
			DeleteContext transientEntities)
			throws HibernateException {
		// I'm not quite sure if we need this method
		return reactiveRemove( entityName, null, isCascadeDeleteEnabled, transientEntities );
	}

	@Override
	public CompletionStage<Void> reactiveRemove(
			String entityName,
			Object child,
			boolean isCascadeDeleteEnabled,
			DeleteContext transientEntities) {
		checkOpenOrWaitingForAutoClose();
		final boolean removingOrphanBeforeUpates = persistenceContext().isRemovingOrphanBeforeUpates();
		if ( log.isTraceEnabled() && removingOrphanBeforeUpates ) {
			logRemoveOrphanBeforeUpdates( "before continuing", entityName, entityName );
		}

		return fireRemove(
				new DeleteEvent( entityName, child, isCascadeDeleteEnabled, removingOrphanBeforeUpates, this ),
				transientEntities
		);
	}

	private ReactivePersistenceContextAdapter persistenceContext() {
		return (ReactivePersistenceContextAdapter) getPersistenceContextInternal();
	}

	private void logRemoveOrphanBeforeUpdates(String timing, String entityName, Object entity) {
		if ( log.isTraceEnabled() ) {
			final EntityEntry entityEntry = persistenceContext().getEntry( entity );
			log.tracef( "%s remove orphan before updates: [%s]", timing,
						entityEntry == null ? entityName : MessageHelper.infoString( entityName, entityEntry.getId() )
			);
		}
	}

	// Should be similar to fireRemove
	private CompletionStage<Void> fireRemove(DeleteEvent event) {
		pulseTransactionCoordinator();

		return fastSessionServices.eventListenerGroup_DELETE.fireEventOnEachListener(
						event,
						(ReactiveDeleteEventListener l) -> l::reactiveOnDelete
				)
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

		return fastSessionServices.eventListenerGroup_DELETE.fireEventOnEachListener( event, transientEntities,
																					  (ReactiveDeleteEventListener l) -> l::reactiveOnDelete
				)
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

		return fastSessionServices.eventListenerGroup_MERGE
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

		return fastSessionServices.eventListenerGroup_MERGE
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
		return fastSessionServices.eventListenerGroup_AUTO_FLUSH
				.fireEventOnEachListener( event, (DefaultReactiveAutoFlushEventListener l) -> l::reactiveOnAutoFlush )
				.thenApply( v -> event.isFlushRequired() );
	}

	@Override
	public CompletionStage<Void> reactiveForceFlush(EntityEntry entry) {
		if ( log.isDebugEnabled() ) {
			log.debugf(
					"Flushing to force deletion of re-saved object: %s",
					MessageHelper.infoString( entry.getPersister(), entry.getId(), getFactory() )
			);
		}

		if ( getPersistenceContextInternal().getCascadeLevel() > 0 ) {
			return CompletionStages.failedFuture( new ObjectDeletedException(
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
			throw log.flushDuringCascadeIsDangerous();
		}

		return fastSessionServices.eventListenerGroup_FLUSH.fireEventOnEachListener(
						new FlushEvent( this ),
						(ReactiveFlushEventListener l) -> l::reactiveOnFlush
				)
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

		return fastSessionServices.eventListenerGroup_REFRESH.fireEventOnEachListener(
						event,
						(ReactiveRefreshEventListener l) -> l::reactiveOnRefresh
				)
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

		return fastSessionServices.eventListenerGroup_REFRESH
				.fireEventOnEachListener(
						event,
						refreshedAlready,
						(ReactiveRefreshEventListener l) -> l::reactiveOnRefresh
				)
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

	private CompletionStage<Void> fireLock(LockEvent event) {
		pulseTransactionCoordinator();

		return fastSessionServices.eventListenerGroup_LOCK.fireEventOnEachListener(
						event,
						(ReactiveLockEventListener l) -> l::reactiveOnLock
				)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					return returnNullorRethrow( e );
				} );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(
			Class<T> entityClass,
			Object id) {
		return new ReactiveIdentifierLoadAccessImpl<>( entityClass ).load( id );
	}

	@Override
	public <T> CompletionStage<T> reactiveFind(
			Class<T> entityClass,
			Object id,
			LockOptions lockOptions,
			EntityGraph<T> fetchGraph) {
		checkOpen();

		if ( fetchGraph != null ) {
			getLoadQueryInfluencers()
					.getEffectiveEntityGraph()
					.applyGraph( (RootGraphImplementor<T>) fetchGraph, GraphSemantic.FETCH );
		}

//		Boolean readOnly = properties == null ? null : (Boolean) properties.get( QueryHints.HINT_READONLY );
//		getLoadQueryInfluencers().setReadOnly( readOnly );

		final ReactiveIdentifierLoadAccessImpl<T> loadAccess =
				new ReactiveIdentifierLoadAccessImpl<>( entityClass )
						.with( determineAppropriateLocalCacheMode( null ) )
						.with( lockOptions );

		return loadAccess.load( id )
				.handle( (result, e) -> {
					if ( e instanceof EntityNotFoundException ) {
						// DefaultLoadEventListener.returnNarrowedProxy may throw ENFE (see HHH-7861 for details),
						// which find() should not throw. Find() should return null if the entity was not found.
						//			if ( log.isDebugEnabled() ) {
						//				String entityName = entityClass != null ? entityClass.getName(): null;
						//				String identifierValue = id != null ? id.toString() : null ;
						//				log.ignoringEntityNotFound( entityName, identifierValue );
						//			}
						throw new NotYetImplementedFor6Exception();
					}
					if ( e instanceof ObjectDeletedException ) {
						//the spec is silent about people doing remove() find() on the same PC
						throw new NotYetImplementedFor6Exception();
					}
					if ( e instanceof ObjectNotFoundException ) {
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
//							throw new NotYetImplementedFor6Exception();
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
		return new ReactiveMultiIdentifierLoadAccessImpl<>( entityClass ).multiLoad( ids );
		//TODO: copy/paste the exception handling from immediately above?
	}

	@Override
	public <T> CompletionStage<T> reactiveFind(Class<T> entityClass, Map<String, Object> ids) {
		EntityPersister persister = getFactory().getMetamodel().locateEntityPersister( entityClass );
		return new NaturalIdLoadAccessImpl<T>( persister ).resolveNaturalId( ids )
				.thenCompose( id -> reactiveFind( entityClass, id, null, null ) );
	}

	private CompletionStage<Void> fireReactiveLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
		checkOpenOrWaitingForAutoClose();

		return fireLoadNoChecks( event, loadType )
				.whenComplete( (v, e) -> delayedAfterCompletion() );
	}

	private CompletionStage<Void> fireLoadNoChecks(LoadEvent event, LoadEventListener.LoadType loadType) {
		pulseTransactionCoordinator();

		return fastSessionServices.eventListenerGroup_LOAD.fireEventOnEachListener( event, loadType,
																					(ReactiveLoadEventListener l) -> l::reactiveOnLoad
		);
	}

	private CompletionStage<Void> fireResolveNaturalId(ResolveNaturalIdEvent event) {
		checkOpenOrWaitingForAutoClose();
		return fastSessionServices.eventListenerGroup_RESOLVE_NATURAL_ID.fireEventOnEachListener(
						event,
						(ReactiveResolveNaturalIdEventListener l) -> l::onReactiveResolveNaturalId
				)
				.whenComplete( (c, e) -> delayedAfterCompletion() );
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

	private Boolean getReadOnlyFromLoadQueryInfluencers() {
		return getLoadQueryInfluencers().getReadOnly();
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

		public final CompletionStage<T> getReference(Object id) {
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
		protected CompletionStage<T> doGetReference(Object id) {
			if ( lockOptions != null ) {
				LoadEvent event = new LoadEvent(
						id,
						entityPersister.getEntityName(),
						lockOptions,
						ReactiveSessionImpl.this,
						getReadOnlyFromLoadQueryInfluencers()
				);
				return fireReactiveLoad( event, LoadEventListener.LOAD ).thenApply( v -> (T) event.getResult() );
			}

			LoadEvent event = new LoadEvent(
					id,
					entityPersister.getEntityName(),
					false,
					ReactiveSessionImpl.this,
					getReadOnlyFromLoadQueryInfluencers()
			);
			return fireReactiveLoad( event, LoadEventListener.LOAD )
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

		public final CompletionStage<T> load(Object id) {
			return perform( () -> doLoad( id, LoadEventListener.GET ) );
		}

		//		public final CompletionStage<T> fetch(Object id) {
//			return perform( () -> doLoad( id, LoadEventListener.IMMEDIATE_LOAD) );
//		}
//
		@SuppressWarnings("unchecked")
		protected final CompletionStage<T> doLoad(Object id, LoadEventListener.LoadType loadType) {
			if ( id == null ) {
				return nullFuture();
			}
			if ( lockOptions != null ) {
				LoadEvent event = new LoadEvent(
						id,
						entityPersister.getEntityName(),
						lockOptions,
						ReactiveSessionImpl.this,
						getReadOnlyFromLoadQueryInfluencers()
				);
				return fireReactiveLoad( event, loadType ).thenApply( v -> (T) event.getResult() );
			}
			LoadEvent event = new LoadEvent(
					id,
					entityPersister.getEntityName(),
					false,
					ReactiveSessionImpl.this,
					getReadOnlyFromLoadQueryInfluencers()
			);
			return fireReactiveLoad( event, loadType )
					.whenComplete( (v, t) -> afterOperation( t != null ) )
					.thenApply( v -> (T) event.getResult() );
		}
	}

	private class ReactiveMultiIdentifierLoadAccessImpl<T> implements MultiIdLoadOptions {
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
			Object[] sids = new Object[ids.length];
			System.arraycopy( ids, 0, sids, 0, ids.length );

			throw new UnsupportedOperationException( "Not yet implemented" );
			// TODO:
//			return perform( () -> (CompletionStage)
//					((ReactiveEntityPersister) entityPersister)
//							.reactiveMultiLoad( sids, ReactiveSessionImpl.this, this ) );
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
		public <K extends Object> CompletionStage<List<T>> multiLoad(List<K> ids) {
			return perform( () -> (CompletionStage<List<T>>)
					entityPersister.multiLoad( ids.toArray( new Object[0] ), ReactiveSessionImpl.this, this ) );
		}
	}

	private class NaturalIdLoadAccessImpl<T> {
		private final EntityPersister entityPersister;
		private LockOptions lockOptions;
		private boolean synchronizationEnabled = true;

		private NaturalIdLoadAccessImpl(EntityPersister entityPersister) {
			this.entityPersister = entityPersister;

			if ( !entityPersister.hasNaturalIdentifier() ) {
				throw log.entityDidNotDefinedNaturalId( entityPersister.getEntityName() );
			}
		}

		public NaturalIdLoadAccessImpl<T> with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		protected void synchronizationEnabled(boolean synchronizationEnabled) {
			this.synchronizationEnabled = synchronizationEnabled;
		}

		protected final CompletionStage<Object> resolveNaturalId(Map<String, Object> naturalIdParameters) {
			performAnyNeededCrossReferenceSynchronizations();

			ResolveNaturalIdEvent event =
					new ResolveNaturalIdEvent( naturalIdParameters, entityPersister, ReactiveSessionImpl.this );
			return fireResolveNaturalId( event )
					.thenApply( v -> event.getEntityId() == INVALID_NATURAL_ID_REFERENCE ? null : event.getEntityId() );
		}

		protected void performAnyNeededCrossReferenceSynchronizations() {
			if ( !synchronizationEnabled ) {
				// synchronization (this process) was disabled
				return;
			}
			if ( entityPersister.getEntityMetamodel().hasImmutableNaturalId() ) {
				// only mutable natural-ids need this processing
				return;
			}
			if ( !isTransactionInProgress() ) {
				// not in a transaction so skip synchronization
				return;
			}

			final PersistenceContext persistenceContext = getPersistenceContextInternal();
//			final boolean debugEnabled = log.isDebugEnabled();
			for ( Object pk : persistenceContext.getNaturalIdResolutions()
					.getCachedPkResolutions( entityPersister ) ) {
				final EntityKey entityKey = generateEntityKey( pk, entityPersister );
				final Object entity = persistenceContext.getEntity( entityKey );
				final EntityEntry entry = persistenceContext.getEntry( entity );

				if ( entry == null ) {
//					if ( debugEnabled ) {
//						log.debug(
//								"Cached natural-id/pk resolution linked to null EntityEntry in persistence context : "
//										+ MessageHelper.infoString( entityPersister, pk, getFactory() )
//						);
//					}
					continue;
				}

				if ( !entry.requiresDirtyCheck( entity ) ) {
					continue;
				}

				// MANAGED is the only status we care about here...
				if ( entry.getStatus() != Status.MANAGED ) {
					continue;
				}

				persistenceContext.getNaturalIdResolutions()
						.handleSynchronization( pk, entity, entityPersister );
			}
		}

		protected final ReactiveIdentifierLoadAccessImpl getIdentifierLoadAccess() {
			final ReactiveIdentifierLoadAccessImpl identifierLoadAccess = new ReactiveIdentifierLoadAccessImpl(
					entityPersister );
			if ( this.lockOptions != null ) {
				identifierLoadAccess.with( lockOptions );
			}
			return identifierLoadAccess;
		}

		protected EntityPersister entityPersister() {
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
		throw new UnsupportedOperationException( "Non reactive close method called. Use 'reactiveClose()' instead." );
	}

	@Override
	public CompletionStage<Void> reactiveClose() {
		super.close();
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
		if ( entity instanceof HibernateProxy ) {
			return (Class<T>) ( (HibernateProxy) entity )
					.getHibernateLazyInitializer()
					.getPersistentClass();
		}
		else {
			return (Class<T>) getEntityPersister( null, entity )
					.getMappedClass();
		}
	}

	@Override
	public Object getEntityId(Object entity) {
		if ( entity instanceof HibernateProxy ) {
			return ( (HibernateProxy) entity ).getHibernateLazyInitializer()
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
		final StatefulPersistenceContext persistenceContext = (StatefulPersistenceContext) getPersistenceContextInternal();
		persistenceContext.beginRemoveOrphanBeforeUpdates();
		return fireRemove( new DeleteEvent( entityName, child, false, true, this ) )
				.thenAccept( v -> {
					persistenceContext.endRemoveOrphanBeforeUpdates();
					if ( log.isTraceEnabled() ) {
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
			StatefulPersistenceContext persistenceContext) {
		if ( log.isTraceEnabled() ) {
			final EntityEntry entityEntry = persistenceContext.getEntry( entity );
			log.tracef(
					"%s remove orphan before updates: [%s]",
					timing,
					entityEntry == null ? entityName : MessageHelper.infoString( entityName, entityEntry.getId() )
			);
		}
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity, String name) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> entity, String name) {
		throw new NotYetImplementedFor6Exception();
	}
}

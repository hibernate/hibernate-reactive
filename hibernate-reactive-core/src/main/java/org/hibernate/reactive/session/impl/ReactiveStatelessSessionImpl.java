/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.SessionException;
import org.hibernate.UnknownEntityTypeException;
import org.hibernate.UnresolvableObjectException;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.internal.ReactivePersistenceContextAdapter;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.monitor.spi.DiagnosticEvent;
import org.hibernate.event.monitor.spi.EventMonitor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.internal.RootGraphImpl;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.StatelessSessionImpl;
import org.hibernate.jpa.spi.NativeQueryTupleTransformer;
import org.hibernate.loader.ast.spi.CascadingFetchProfile;
import org.hibernate.persister.entity.EntityPersister;
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
import org.hibernate.query.sql.spi.NativeQueryImplementor;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.query.sqm.tree.select.SqmQueryGroup;
import org.hibernate.query.sqm.tree.select.SqmQuerySpec;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
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
import org.hibernate.reactive.query.sqm.internal.ReactiveQuerySqmImpl;
import org.hibernate.reactive.query.sqm.internal.ReactiveSqmSelectionQueryImpl;
import org.hibernate.reactive.session.ReactiveSqmQueryImplementor;
import org.hibernate.reactive.session.ReactiveStatelessSession;
import org.hibernate.reactive.util.impl.CompletionStages.Completable;
import org.hibernate.stat.spi.StatisticsImplementor;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.Tuple;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CommonAbstractCriteria;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.lang.Boolean.TRUE;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.function.Function.identity;
import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.Versioning.incrementVersion;
import static org.hibernate.engine.internal.Versioning.seedVersion;
import static org.hibernate.engine.internal.Versioning.setVersion;
import static org.hibernate.event.internal.DefaultInitializeCollectionEventListener.handlePotentiallyEmptyCollection;
import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.internal.util.NullnessUtil.castNonNull;
import static org.hibernate.internal.util.StringHelper.isEmpty;
import static org.hibernate.internal.util.StringHelper.isNotEmpty;
import static org.hibernate.loader.ast.spi.CascadingFetchProfile.REFRESH;
import static org.hibernate.loader.internal.CacheLoadHelper.initializeCollectionFromCache;
import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.proxy.HibernateProxy.extractLazyInitializer;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.castToIdentifierType;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister.forceInitialize;
import static org.hibernate.reactive.session.impl.SessionUtil.checkEntityFound;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * An {@link ReactiveStatelessSession} implemented by extension of
 * the {@link StatelessSessionImpl} in Hibernate core. Extension was
 * preferred to delegation because there are places where
 * Hibernate core compares the identity of session instances.
 */
public class ReactiveStatelessSessionImpl extends StatelessSessionImpl implements ReactiveStatelessSession {

	private static final Log LOG = make( Log.class, lookup() );

	private final LoadQueryInfluencers influencers;
	private final ReactiveConnection reactiveConnection;
	private final ReactiveStatelessSessionImpl batchingHelperSession;
	private final PersistenceContext persistenceContext;

	public ReactiveStatelessSessionImpl(SessionFactoryImpl factory, SessionCreationOptions options, ReactiveConnection connection) {
		super( factory, options );
		reactiveConnection = connection;
		persistenceContext = new ReactivePersistenceContextAdapter( super.getPersistenceContext() );
		batchingHelperSession = new ReactiveStatelessSessionImpl( factory, options, reactiveConnection, persistenceContext );
		influencers = new LoadQueryInfluencers( factory );
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
		// StatelessSession should not allow JDBC batching, because that would change
		// its "immediate synchronous execution" model into something more like transactional
		// write-behind and be confusing. For this reason, the default batch size is always set to 0.
		// When a user calls the CRUD operations for batching, we set the batch size to the same number of
		// objects to process, therefore, there is no write-behind behavior.
		reactiveConnection = new BatchingConnection( connection, 0 );
		batchingHelperSession = this;
		influencers = new LoadQueryInfluencers( factory );
	}

	private LockMode getNullSafeLockMode(LockMode lockMode) {
		return lockMode == null ? LockMode.NONE : lockMode;
	}

	@Override
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
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
	public <T> CompletionStage<T> reactiveGet(Class<T> entityClass, Object id) {
		return reactiveGet( entityClass.getName(), id, LockMode.NONE, null );
	}

	@Override
	public <T> CompletionStage<List<T>> reactiveGet(Class<T> entityClass, Object... ids) {
		checkOpen();
		for (Object id : ids) {
			if ( id == null ) {
				return failedFuture( new IllegalArgumentException( "Null id" ) );
			}
		}

		Object[] sids = new Object[ids.length];
		System.arraycopy( ids, 0, sids, 0, ids.length );

		return getEntityPersister( entityClass.getName() )
				.reactiveMultiLoad( sids, this, StatelessSessionImpl.MULTI_ID_LOAD_OPTIONS )
				.whenComplete( (list, e) -> {
					if ( getPersistenceContext().isLoadFinished() ) {
						getPersistenceContext().clear();
					}
				} )
				.thenApply( list -> (List<T>) list );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(String entityName, Object id) {
		return reactiveGet( entityName, id, LockMode.NONE, null );
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(Class<T> entityClass, Object id, LockMode lockMode, EntityGraph<T> fetchGraph) {
		return reactiveGet( entityClass.getName(), id, lockMode, fetchGraph );
	}

	@Override
	public LoadQueryInfluencers getLoadQueryInfluencers() {
		return influencers;
	}

	@Override
	public <T> CompletionStage<T> reactiveGet(String entityName, Object id, LockMode lockMode, EntityGraph<T> fetchGraph) {
		checkOpen();

		// differs from core, because core doesn't let us pass an EntityGraph
		if ( fetchGraph != null ) {
			getLoadQueryInfluencers()
					.getEffectiveEntityGraph()
					.applyGraph( (RootGraphImplementor<T>) fetchGraph, GraphSemantic.FETCH );
		}

		ReactiveEntityPersister persister = getEntityPersister( entityName );
		if ( persister.canReadFromCache() ) {
			final Object cachedEntity = loadFromSecondLevelCache( persister, generateEntityKey( id, persister ), null, lockMode );
			if ( cachedEntity != null ) {
				getPersistenceContext().clear();
				return completedFuture( (T) cachedEntity );
			}
		}
		return persister
				.reactiveLoad( id, null, getNullSafeLockMode( lockMode ), this )
				.whenComplete( (v, e) -> {
					if ( getPersistenceContext().isLoadFinished() ) {
						getPersistenceContext().clear();
					}
					getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
				} )
				.thenApply( entity -> (T) entity );
	}

	private ReactiveEntityPersister getEntityPersister(String entityName) {
		return (ReactiveEntityPersister) getFactory().getMappingMetamodel()
				.getEntityDescriptor( entityName );
	}

	@Override
	public ReactiveEntityPersister getEntityPersister(String entityName, Object object) throws HibernateException {
		return (ReactiveEntityPersister) super.getEntityPersister( entityName, object );
	}

	@Override
	public CompletionStage<Void> reactiveInsert(Object entity) {
		checkOpen();
		final ReactiveEntityPersister persister = getEntityPersister( null, entity );
		final Object[] state = persister.getValues( entity );
		return reactiveInsert( entity, state, persister )
				.thenCompose( id -> recreateCollections( entity, id, persister ) )
				.thenAccept( id -> {
					firePostInsert( entity, id, state, persister );
					final StatisticsImplementor statistics = getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.insertEntity( persister.getEntityName() );
					}
				} );
	}

	private static class Loop {
		private CompletionStage<Void> loop = voidFuture();

		public void then(Supplier<CompletionStage<Void>> step) {
			loop = loop.thenCompose( v -> step.get() );
		}

		public void whenComplete(BiConsumer<Void, Throwable> consumer) {
			loop = loop.whenComplete( consumer );
		}
	}

	private CompletionStage<Void> recreateCollections(Object entity, Object id, EntityPersister persister) {
		final Completable<Void> stage = new Completable<>();
		final String entityName = persister.getEntityName();
		final EventMonitor eventMonitor = getEventMonitor();
		final Loop loop = new Loop();
		forEachOwnedCollection(
				entity, id, persister, (descriptor, collection) -> {
					firePreRecreate( collection, descriptor, entityName, entity );
					final DiagnosticEvent event = eventMonitor.beginCollectionRecreateEvent();
					loop.then( () -> supplyStage( () -> ( (ReactiveCollectionPersister) descriptor )
							.reactiveRecreate( collection, id, this ) )
							.whenComplete( (t, throwable) -> eventMonitor
									.completeCollectionRecreateEvent( event, id, descriptor.getRole(), throwable != null, this )
							)
							.thenAccept( unused -> {
								final StatisticsImplementor statistics = getFactory().getStatistics();
								if ( statistics.isStatisticsEnabled() ) {
									statistics.recreateCollection( descriptor.getRole() );
								}
								firePostRecreate( collection, id, entityName, descriptor );
							} )
					);
				}
		);
		loop.whenComplete( stage::complete );
		return stage.getStage();
	}

	private CompletionStage<?> reactiveInsert(Object entity, Object[] state, ReactiveEntityPersister persister) {
		if ( persister.isVersioned() ) {
			if ( seedVersion( entity, state, persister, this ) ) {
				persister.setValues( entity, state );
			}
		}
		final Generator generator = persister.getGenerator();
		if ( generator.generatedBeforeExecution( entity, this ) ) {
			return generatedIdBeforeInsert( entity, persister, generator, state );
		}
		if ( generator.generatedOnExecution( entity, this ) ) {
			return generateIdOnInsert( entity, persister, generator, state );
		}
		return applyAssignedIdentifierInsert( entity, persister, state );
	}

	private CompletionStage<?> applyAssignedIdentifierInsert(Object entity, ReactiveEntityPersister persister, Object[] state) {
		Object id = persister.getIdentifier( entity, this );
		if ( id == null ) {
			return failedFuture( new IdentifierGenerationException( "Identifier of entity '" + persister.getEntityName() + "' must be manually assigned before calling 'insert()'" ) );
		}
		if ( firePreInsert( entity, id, state, persister ) ) {
			return completedFuture( id );
		}
		getInterceptor().onInsert( entity, id, state, persister.getPropertyNames(), persister.getPropertyTypes() );
		final EventMonitor eventMonitor = getEventMonitor();
		final DiagnosticEvent event = eventMonitor.beginEntityInsertEvent();
		// try-block
		return supplyStage( () -> persister.insertReactive( id, state, entity, this ) )
				// finally: catches error in case insertReactive fails before returning a CompletionStage
				.whenComplete( (generatedValues, throwable) -> eventMonitor
						.completeEntityInsertEvent( event, id, persister.getEntityName(), throwable != null, this )
				);
	}

	private CompletionStage<?> generateIdOnInsert(
			Object entity,
			ReactiveEntityPersister persister,
			Generator generator,
			Object[] state) {
		if ( !generator.generatesOnInsert() ) {
			throw new IdentifierGenerationException( "Identifier generator must generate on insert" );
		}
		if ( firePreInsert( entity, null, state, persister ) ) {
			return nullFuture();
		}
		getInterceptor().onInsert( entity, null, state, persister.getPropertyNames(), persister.getPropertyTypes() );
		final EventMonitor eventMonitor = getEventMonitor();
		final DiagnosticEvent event = eventMonitor.beginEntityInsertEvent();
		// try-block
		return supplyStage( () -> persister
				.insertReactive( state, entity, this )
				.thenApply( generatedValues -> castNonNull( generatedValues )
						.getGeneratedValue( persister.getIdentifierMapping() )
				) )
				// finally-block: catch the exceptions from insertReactive and getGeneratedValues
				.whenComplete( (id, throwable) -> eventMonitor
						.completeEntityInsertEvent( event, id, persister.getEntityName(), throwable != null, this )
				)
				.thenApply( id -> {
					persister.setIdentifier( entity, id, this );
					return id;
				} );
	}

	private CompletionStage<?> generatedIdBeforeInsert(
			Object entity,
			ReactiveEntityPersister persister,
			Generator generator,
			Object[] state) {
		if ( !generator.generatesOnInsert() ) {
			return failedFuture( new IdentifierGenerationException( "Identifier generator must generate on insert" ) );
		}
		return generateIdForInsert( entity, generator, persister )
				.thenCompose( id -> {
					persister.setIdentifier( entity, id, this );
					if ( firePreInsert( entity, id, state, persister ) ) {
						return completedFuture( id );
					}
					getInterceptor().onInsert( entity, id, state, persister.getPropertyNames(), persister.getPropertyTypes() );
					final EventMonitor eventMonitor = getEventMonitor();
					final DiagnosticEvent event = eventMonitor.beginEntityInsertEvent();
					// try-block
					return supplyStage( () -> persister.insertReactive( id, state, entity, this ) )
							// finally: catches error in case insertReactive fails before returning a CompletionStage
							.whenComplete( (generatedValues, throwable) -> eventMonitor
									.completeEntityInsertEvent( event, id, persister.getEntityName(), throwable != null, this )
							)
							.thenApply( identity() );
				} );
	}

	private CompletionStage<?> generateIdForInsert(Object entity, Generator generator, ReactiveEntityPersister persister) {
		if ( generator instanceof ReactiveIdentifierGenerator<?> reactiveGenerator ) {
			return reactiveGenerator.generate( this, this )
					.thenApply( id -> castToIdentifierType( id, persister ) );
		}

		final Object currentValue = generator.allowAssignedIdentifiers() ? persister.getIdentifier( entity ) : null;
		return completedFuture( ( (BeforeExecutionGenerator) generator ).generate( this, entity, currentValue, INSERT ) );
	}

	@Override
	public CompletionStage<Void> reactiveDelete(Object entity) {
		checkOpen();
		final ReactiveEntityPersister persister = getEntityPersister( null, entity );
		final Object id = persister.getIdentifier( entity, this );
		final Object version = persister.getVersion( entity );
		if ( firePreDelete( entity, id, persister ) ) {
			return voidFuture();
		}

		getInterceptor().onDelete( entity, id, persister.getPropertyNames(), persister.getPropertyTypes() );
		return removeCollections( entity, id, persister )
				.thenCompose( v -> {
					final Object ck = lockCacheItem( id, version, persister );
					final EventMonitor eventMonitor = getEventMonitor();
					final DiagnosticEvent event = eventMonitor.beginEntityDeleteEvent();
					// try-block
					return supplyStage( () -> persister.deleteReactive( id, version, entity, this ) )
							// finally-block
							.whenComplete( (unused, throwable) -> eventMonitor
									.completeEntityDeleteEvent( event, id, persister.getEntityName(), throwable != null, this )
							)
							.thenAccept( unused -> removeCacheItem( ck, persister ) );
				} )
				.thenAccept( v -> {
					firePostDelete( entity, id, persister );
					final StatisticsImplementor statistics = getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.deleteEntity( persister.getEntityName() );
					}
				} );
	}

	private CompletionStage<Void> removeCollections(Object entity, Object id, EntityPersister persister) {
		if ( persister.hasOwnedCollections() ) {
			final Loop loop = new Loop();
			final Completable<Void> stage = new Completable<>();
			final String entityName = persister.getEntityName();
			forEachOwnedCollection(
					entity, id, persister,
					(descriptor, collection) -> {
						firePreRemove( collection, id, entityName, entity );
						final EventMonitor eventMonitor = getEventMonitor();
						final DiagnosticEvent event = eventMonitor.beginCollectionRemoveEvent();
						loop.then( () -> supplyStage( () -> ( (ReactiveCollectionPersister) descriptor )
								.reactiveRemove( id, this ) )
								.whenComplete( (unused, throwable) -> eventMonitor
										.completeCollectionRemoveEvent( event, id, descriptor.getRole(), throwable != null, this )
								)
								.thenAccept( v -> {
									firePostRemove( collection, id, entityName, entity );
									final StatisticsImplementor statistics = getFactory().getStatistics();
									if ( statistics.isStatisticsEnabled() ) {
										statistics.removeCollection( descriptor.getRole() );
									}
								} )
						);
					}
			);
			loop.whenComplete( stage::complete );
			return stage.getStage();
		}
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveUpdate(Object entity) {
		checkOpen();
		if ( entity instanceof HibernateProxy proxy ) {
			final LazyInitializer hibernateLazyInitializer = proxy.getHibernateLazyInitializer();
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
		final ReactiveEntityPersister persister = getEntityPersister( null, entity );
		final Object id = persister.getIdentifier( entity, this );
		final Object[] state = persister.getValues( entity );
		final Object oldVersion = persister.isVersioned() ? persister.getVersion( entity ) : null;
		if ( persister.isVersioned() ) {
			final Object newVersion = incrementVersion( entity, oldVersion, persister, this );
			setVersion( state, newVersion, persister );
			persister.setValues( entity, state );
		}

		if ( firePreUpdate(entity, id, state, persister) ) {
			return voidFuture();
		}

		getInterceptor().onUpdate( entity, id, state, persister.getPropertyNames(), persister.getPropertyTypes() );
		final Object ck = lockCacheItem( id, oldVersion, persister );
		final EventMonitor eventMonitor = getEventMonitor();
		final DiagnosticEvent event = eventMonitor.beginEntityUpdateEvent();
		// try-block
		return supplyStage( () -> persister
				.updateReactive( id, state, null, false, null, oldVersion, entity, null, this ) )
				// finally-block
				.whenComplete( (generatedValues, throwable) -> eventMonitor
						.completeEntityUpdateEvent( event, id, persister.getEntityName(), throwable != null, this )
				)
				.thenAccept( generatedValues -> removeCacheItem( ck, persister ) )
				.thenCompose( v -> removeAndRecreateCollections( entity, id, persister ) )
				.thenAccept( v -> {
					firePostUpdate( entity, id, state, persister );
					final StatisticsImplementor statistics = getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.updateEntity( persister.getEntityName() );
					}
				} );
	}

	private CompletionStage<Void> removeAndRecreateCollections(Object entity, Object id, EntityPersister persister) {
		if ( persister.hasOwnedCollections() ) {
			final String entityName = persister.getEntityName();
			final Completable<Void> stage = new Completable<>();
			final Loop loop = new Loop();
			forEachOwnedCollection(
					entity, id, persister,
					(descriptor, collection) -> {
						firePreUpdate( collection, id, entityName, entity );
						final EventMonitor eventMonitor = getEventMonitor();
						final DiagnosticEvent event = eventMonitor.beginCollectionRemoveEvent();
						ReactiveCollectionPersister reactivePersister = (ReactiveCollectionPersister) persister;
						loop.then( () -> supplyStage( () -> reactivePersister
								.reactiveRemove( id, this )
								.thenCompose( v -> reactivePersister.reactiveRecreate( collection, id, this ) ) )
								.whenComplete( (unused, throwable) -> eventMonitor
										.completeCollectionRemoveEvent( event, id, descriptor.getRole(), throwable != null, this )
								)
								.thenAccept( v -> {
									firePostUpdate( collection, id, entityName, entity);
									final StatisticsImplementor statistics = getFactory().getStatistics();
									if ( statistics.isStatisticsEnabled() ) {
										statistics.updateCollection( descriptor.getRole() );
									}
								} )
						);
					}
			);
			loop.whenComplete( stage::complete );
			return stage.getStage();
		}
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object entity) {
		return reactiveRefresh( bestGuessEntityName( entity ), entity, LockMode.NONE );
	}

	@Override
	public CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode) {
		return reactiveRefresh( bestGuessEntityName( entity ), entity, lockMode );
	}

	private CompletionStage<Void> reactiveRefresh(String entityName, Object entity, LockMode lockMode) {
		checkOpen();
		final ReactiveEntityPersister persister = getEntityPersister( entityName, entity );
		final Object id = persister.getIdentifier( entity, this );

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Refreshing transient {0}", infoString( persister, id, getFactory() ) );
		}

		if ( persister.canWriteToCache() ) {
			final EntityDataAccess cacheAccess = persister.getCacheAccessStrategy();
			if ( cacheAccess != null ) {
				final Object ck = cacheAccess.generateCacheKey( id, persister, getFactory(), getTenantIdentifier() );
				cacheAccess.evict( ck );
			}
		}

		return fromInternalFetchProfile( REFRESH, () -> persister.reactiveLoad( id, entity, getNullSafeLockMode( lockMode ), this ) )
				.thenAccept( result -> {
					UnresolvableObjectException.throwIfNull( result, id, persister.getEntityName() );
					if ( getPersistenceContext().isLoadFinished() ) {
						getPersistenceContext().clear();
					}
				} );
	}

	private CompletionStage<?> fromInternalFetchProfile(CascadingFetchProfile cascadingFetchProfile, Supplier<CompletionStage<?>> supplier) {
		CascadingFetchProfile previous = getLoadQueryInfluencers().getEnabledCascadingFetchProfile();
		return supplyStage( () -> {
					getLoadQueryInfluencers().setEnabledCascadingFetchProfile( cascadingFetchProfile );
					return supplier.get();
				} )
				.whenComplete( (o, throwable) -> getLoadQueryInfluencers().setEnabledCascadingFetchProfile( previous ) );
	}

	/**
	 * @see StatelessSessionImpl#upsert(Object)
	 */
	@Override
	public CompletionStage<Void> reactiveUpsert(Object entity) {
		checkOpen();
		final ReactiveEntityPersister persister = getEntityPersister( null, entity );
		final Object id = idToUpsert( entity, persister );
		final Object[] state = persister.getValues( entity );
		if ( firePreUpsert( entity, id, state, persister ) ) {
			return voidFuture();
		}
		getInterceptor().onUpsert( entity, id, state, persister.getPropertyNames(), persister.getPropertyTypes() );
		final Object oldVersion = versionToUpsert( entity, persister, state );
		final Object ck = lockCacheItem( id, oldVersion, persister );
		final EventMonitor eventMonitor = getEventMonitor();
		final DiagnosticEvent event = eventMonitor.beginEntityUpsertEvent();
		return supplyStage( () -> persister
				.mergeReactive( id, state, null, false, null, oldVersion, entity, null, this ) )
				.whenComplete( (v, throwable) -> eventMonitor
						.completeEntityUpsertEvent( event, id, persister.getEntityName(), throwable != null, this )
				)
				.thenAccept( v -> {
					removeCacheItem( ck, persister );
					final StatisticsImplementor statistics = getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.upsertEntity( persister.getEntityName() );
					}
				} )
				.thenCompose( v -> removeAndRecreateCollections( entity, id, persister ) )
				.thenAccept( v -> firePostUpsert( entity, id, state, persister ) );
	}

	@Override
	public CompletionStage<Void> reactiveUpsertAll(int batchSize, Object... entities) {
		final Integer jdbcBatchSize = batchingHelperSession.getJdbcBatchSize();
		batchingHelperSession.setJdbcBatchSize( batchSize );
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveUpsert )
				.thenCompose( v -> connection.executeBatch() )
				.whenComplete( (v, throwable) -> batchingHelperSession.setJdbcBatchSize( jdbcBatchSize ) );
	}

	@Override
	public CompletionStage<Void> reactiveInsertAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveInsert )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveInsertAll(int batchSize, Object... entities) {
		final Integer jdbcBatchSize = batchingHelperSession.getJdbcBatchSize();
		batchingHelperSession.setJdbcBatchSize( batchSize );
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveInsert )
				.thenCompose( v -> connection.executeBatch() )
				.whenComplete( (v, throwable) -> batchingHelperSession.setJdbcBatchSize( jdbcBatchSize ) );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveUpdate )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateAll(int batchSize, Object... entities) {
		final Integer jdbcBatchSize = batchingHelperSession.getJdbcBatchSize();
		batchingHelperSession.setJdbcBatchSize( batchSize );
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveUpdate )
				.thenCompose( v -> connection.executeBatch() )
				.whenComplete( (v, throwable) -> batchingHelperSession.setJdbcBatchSize( jdbcBatchSize ) );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveDelete )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAll(int batchSize, Object... entities) {
		final Integer jdbcBatchSize = batchingHelperSession.getJdbcBatchSize();
		batchingHelperSession.setJdbcBatchSize( batchSize );
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveDelete ).thenCompose( v -> connection.executeBatch() )
				.whenComplete( (v, throwable) -> batchingHelperSession.setJdbcBatchSize( jdbcBatchSize ) );
	}

	@Override
	public CompletionStage<Void> reactiveRefreshAll(Object... entities) {
		return loop( entities, batchingHelperSession::reactiveRefresh )
				.thenCompose( v -> batchingHelperSession.getReactiveConnection().executeBatch() );
	}

	@Override
	public CompletionStage<Void> reactiveRefreshAll(int batchSize, Object... entities) {
		final Integer jdbcBatchSize = batchingHelperSession.getJdbcBatchSize();
		batchingHelperSession.setJdbcBatchSize( batchSize );
		final ReactiveConnection connection = batchingConnection( batchSize );
		return loop( entities, batchingHelperSession::reactiveRefresh )
				.thenCompose( v -> connection.executeBatch() )
				.whenComplete( (v, throwable) -> batchingHelperSession.setJdbcBatchSize( jdbcBatchSize ) );
	}

	private ReactiveConnection batchingConnection(int batchSize) {
		return batchingHelperSession.getReactiveConnection()
				.withBatchSize( batchSize );
	}

	@Override
	public CompletionStage<Object> reactiveInternalLoad(String entityName, Object id, boolean eager, boolean nullable) {
		Object object = super.internalLoad( entityName, id, eager, nullable );
		return object instanceof CompletionStage<?>
				? (CompletionStage<Object>) object
				: completedFuture( object );
	}

	@Override
	public CompletionStage<Object> reactiveImmediateLoad(String entityName, Object id) {
		if ( persistenceContext.isLoadFinished() ) {
			throw new SessionException( "proxies cannot be fetched by a stateless session" );
		}
		// unless we are still in the process of handling a top-level load
		return reactiveGet( entityName, id );
	}

	@Override
	protected Object internalLoadGet(String entityName, Object id, PersistenceContext persistenceContext) {
		// otherwise immediately materialize it

		// IMPLEMENTATION NOTE: increment/decrement the load count before/after getting the value
		//                      to ensure that #get does not clear the PersistenceContext.
		persistenceContext.beforeLoad();
		return this.reactiveGet( entityName, id )
				.whenComplete( (r, e) -> persistenceContext.afterLoad() );
	}

	@Override
	public CompletionStage<Void> reactiveInitializeCollection(PersistentCollection<?> collection, boolean writing) {
		checkOpen();
		final CollectionEntry ce = persistenceContext.getCollectionEntry( collection );
		if ( ce == null ) {
			throw new HibernateException( "no entry for collection" );
		}
        if ( collection.wasInitialized() ) {
            return voidFuture();
        }
		else {
            final ReactiveCollectionPersister loadedPersister =
                    (ReactiveCollectionPersister) ce.getLoadedPersister();
            final Object loadedKey = ce.getLoadedKey();
            if ( LOG.isTraceEnabled() ) {
                LOG.trace( "Initializing collection "
                        + collectionInfoString( loadedPersister, collection, loadedKey, this ) );
            }
            final boolean foundInCache =
                    initializeCollectionFromCache( loadedKey, loadedPersister, collection, this );
            if ( foundInCache ) {
                LOG.trace( "Collection initialized from cache" );
                return voidFuture();
            }
            else {
                return loadedPersister.reactiveInitialize( loadedKey, this )
                        .thenAccept( v -> {
                            handlePotentiallyEmptyCollection( collection, persistenceContext, loadedKey, loadedPersister );
                            LOG.trace( "Collection initialized" );
                            final StatisticsImplementor statistics = getFactory().getStatistics();
                            if ( statistics.isStatisticsEnabled() ) {
                                statistics.fetchCollection( loadedPersister.getRole() );
                            }
                        } );
            }
        }
    }

	@Override
	public <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy) {
		checkOpen();
		if ( association == null ) {
			return nullFuture();
		}

		final PersistenceContext persistenceContext = getPersistenceContext();
		final LazyInitializer initializer = extractLazyInitializer( association );
		if ( initializer != null ) {
			return initializer.isUninitialized()
					? fetchUninitialized( association, unproxy, initializer, persistenceContext )
					: completedFuture( unproxy ? (T) initializer.getImplementation() : association );
		}
		else if ( isPersistentAttributeInterceptable( association ) ) {
			if ( asPersistentAttributeInterceptable( association ).$$_hibernate_getInterceptor()
					instanceof EnhancementAsProxyLazinessInterceptor proxyInterceptor ) {
				proxyInterceptor.setSession( this );
				return forceInitialize( association, null, proxyInterceptor.getIdentifier(), proxyInterceptor.getEntityName(), this )
						.whenComplete( (i,e) -> {
							proxyInterceptor.unsetSession();
							if ( persistenceContext.isLoadFinished() ) {
								persistenceContext.clear();
							}
						} )
						.thenApply( i -> association );
			}
		}
		else if ( association instanceof PersistentCollection<?> collection && !collection.wasInitialized() ) {
			final ReactiveCollectionPersister collectionDescriptor = (ReactiveCollectionPersister) getFactory()
					.getMappingMetamodel().getCollectionDescriptor( collection.getRole() );

			final Object key = collection.getKey();
			persistenceContext.addUninitializedCollection( collectionDescriptor, collection, key );
			collection.setCurrentSession( this );
			return supplyStage( () -> {
				if ( initializeCollectionFromCache( key, collectionDescriptor, collection, this ) ) {
					LOG.trace( "Collection fetched from cache" );
					return completedFuture( association );
				}
				else {
					return collectionDescriptor
							.reactiveInitialize( key, this )
							.thenApply( v -> {
								handlePotentiallyEmptyCollection( collection, getPersistenceContextInternal(), key, collectionDescriptor );
								LOG.trace( "Collection fetched" );
								final StatisticsImplementor statistics = getFactory().getStatistics();
								if ( statistics.isStatisticsEnabled() ) {
									statistics.fetchCollection( collectionDescriptor.getRole() );
								}
								return association;
							} );
				}
			} ).whenComplete( (t, throwable) -> {
				collection.$$_hibernate_setInstanceId( 0 );
				collection.unsetSession( this );
				if ( persistenceContext.isLoadFinished() ) {
					persistenceContext.clear();
				}
			} );
		}

		return completedFuture( association );
	}

	private <T> CompletionStage<T> fetchUninitialized(
			T association,
			boolean unproxy,
			LazyInitializer initializer,
			PersistenceContext persistenceContext) {
		final String entityName = initializer.getEntityName();
		final Object id = initializer.getIdentifier();
		initializer.setSession( this );
		persistenceContext.beforeLoad();
		return reactiveImplementation( initializer )
				.thenApply( entity -> {
					checkEntityFound( this, entityName, id, entity );
					initializer.setImplementation( entity );
					return unproxy ? (T) entity : association;
				} )
				.whenComplete( (v, e) -> {
					initializer.unsetSession();
					persistenceContext.afterLoad();
					if ( persistenceContext.isLoadFinished() ) {
						persistenceContext.clear();
					}
				} );
	}

	private static CompletionStage<?> reactiveImplementation(LazyInitializer initializer) {
		// This is hard to test because it happens on slower machines like the ones we use on CI.
		// See AbstractLazyInitializer#initialize, it happens when the object is not initialized, and we need to
		// call session.immediateLoad
		return initializer.getImplementation() instanceof CompletionStage
				? (CompletionStage<?>) initializer.getImplementation()
				: completedFuture( initializer.getImplementation() );
	}

	@Override
	public <T> RootGraphImplementor<T> createEntityGraph(Class<T> entity) {
		return new RootGraphImpl<>( null, getFactory().getJpaMetamodel().entity( entity ) );
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
	public <R> ReactiveQuery<R> createReactiveQuery(TypedQueryReference<R> typedQueryReference) {
		checksBeforeQueryCreation();
		if ( typedQueryReference instanceof SelectionSpecificationImpl<R> specification ) {
			final CriteriaQuery<R> query = specification.buildCriteria( getCriteriaBuilder() );
			return new ReactiveQuerySqmImpl<>( (SqmStatement<R>) query, specification.getResultType(), this );
		}
		if ( typedQueryReference instanceof MutationSpecificationImpl<?> specification ) {
			final CommonAbstractCriteria query = specification.buildCriteria( getCriteriaBuilder() );
			return new ReactiveQuerySqmImpl<>( (SqmStatement<R>) query, (Class<R>) specification.getResultType(), this );
		}
		@SuppressWarnings("unchecked")
		// this cast is fine because of all our impls of TypedQueryReference return Class<R>
		final Class<R> resultType = (Class<R>) typedQueryReference.getResultType();
		ReactiveQueryImplementor<R> query = (ReactiveQueryImplementor<R>) buildNamedQuery(
				typedQueryReference.getName(),
				memento -> createSqmQueryImplementor( resultType, memento ),
				memento -> createNativeQueryImplementor( resultType, memento )
		);
		typedQueryReference.getHints().forEach( query::setHint );
		return query;
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

			return createReactiveCriteriaQuery( selectStatement, criteriaQuery.getResultType() );
		}
		catch (RuntimeException e) {
			if ( getSessionFactory().getSessionFactoryOptions().getJpaCompliance().isJpaTransactionComplianceEnabled() ) {
				markForRollbackOnly();
			}
			throw getExceptionConverter().convert( e );
		}
	}

	private <T> ReactiveQuery<T> createReactiveCriteriaQuery(SqmStatement<T> criteria, Class<T> resultType) {
		final ReactiveQuerySqmImpl<T> query = new ReactiveQuerySqmImpl<>( criteria, resultType, this );
		applyQuerySettingsAndHints( query );
		return query;
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
			final HqlInterpretation<?> interpretation = interpretHql( queryString, expectedResultType );
			final ReactiveQuerySqmImpl<R> query =
					new ReactiveQuerySqmImpl<>( queryString, interpretation, expectedResultType, this );
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
			final ReactiveNativeQueryImpl<R> query = new ReactiveNativeQueryImpl<>( sqlString, null, this);
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
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, Class<R> resultClass) {
		final ReactiveNativeQuery<R> query = createReactiveNativeQuery( sqlString );
		handleTupleResultType( resultClass, query );
		addEntityOrResultType( resultClass, query );
		return query;
	}

	private <R> void addEntityOrResultType(Class<R> resultClass, ReactiveNativeQuery<R> query) {
		if ( getFactory().getMappingMetamodel().isEntityClass( resultClass ) ) {
			query.addEntity( "alias1", resultClass.getName(), LockMode.READ );
		}
		else if ( resultClass != Object.class && resultClass != Object[].class && resultClass != Tuple.class ) {
			query.addResultTypeClass( resultClass );
		}
	}

	@Override @Deprecated(forRemoval = true)
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(
			String sqlString,
			Class<R> resultClass,
			String tableAlias) {
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
		checkOpen();
		pulseTransactionCoordinator();
		delayedAfterCompletion();

		try {
			if ( isNotEmpty( resultSetMappingName ) ) {
				final NamedResultSetMappingMemento resultSetMappingMemento = getFactory().getQueryEngine()
						.getNamedObjectRepository()
						.getResultSetMappingMemento( resultSetMappingName );

				if ( resultSetMappingMemento == null ) {
					throw new HibernateException( "Could not resolve specified result-set mapping name : " + resultSetMappingName );
				}
				return new ReactiveNativeQueryImpl<>( sqlString, resultSetMappingMemento, null, this );
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

	@Override @Deprecated(forRemoval = true)
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(
			String sqlString,
			String resultSetMappingName,
			Class<R> resultClass) {
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
	public <R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(CriteriaQuery<R> criteria) {
		SqmUtil.verifyIsSelectStatement( (SqmStatement<R>) criteria, null );
		return new ReactiveSqmSelectionQueryImpl<>( (SqmSelectStatement<R>) criteria, criteria.getResultType(), this );
	}

	@Override
	public <R> ReactiveMutationQuery<R> createReactiveMutationQuery(String hqlString) {
		final QueryImplementor<?> query = createQuery( hqlString );
		final SqmStatement<R> sqmStatement = ( (SqmQueryImplementor<R>) query ).getSqmStatement();
		checkMutationQuery( hqlString, sqmStatement );
		return new ReactiveQuerySqmImpl<>( sqmStatement, null, this );
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

	@Override
	public  <R> ReactiveQueryImplementor<R> createReactiveNamedQuery(String queryName) {
		checksBeforeQueryCreation();
		try {
			return (ReactiveQueryImplementor<R>) buildNamedQuery(
					queryName,
					this::createSqmQueryImplementor,
					this::createNativeQueryImplementor
			);
		}
		catch (RuntimeException e) {
			throw convertNamedQueryException( e );
		}
	}

	@Override
	public <R> ReactiveQueryImplementor<R> createReactiveNamedQuery(String queryName, Class<R> resultType) {
		checksBeforeQueryCreation();
		if ( resultType == null ) {
			throw new IllegalArgumentException( "Result class is null" );
		}
		try {
			return (ReactiveQueryImplementor<R>) buildNamedQuery(
					queryName,
					memento -> createSqmQueryImplementor( resultType, memento ),
					memento -> createNativeQueryImplementor( resultType, memento )
			);
		}
		catch (RuntimeException e) {
			throw convertNamedQueryException( e );
		}
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
	public <R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String queryName, Class<R> expectedResultType) {
		return (ReactiveSelectionQuery<R>) createNamedSelectionQuery( queryName , expectedResultType );
	}

	private void checksBeforeQueryCreation() {
		checkOpen();
		checkTransactionSynchStatus();
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
			final ReactiveNativeQueryImpl<R> query = new ReactiveNativeQueryImpl<>( queryString, this );
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

	@Override
	public <R> ReactiveNativeQuery<R> createReactiveNativeQuery(
			String queryString,
			Class<R> resultType,
			AffectedEntities affectedEntities) {
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

		if ( resultSetMapping == null ) {
			throw new IllegalArgumentException( "Result set mapping was not specified" );
		}

		try {
			final NamedResultSetMappingMemento memento = getResultSetMappingMemento( resultSetMapping.getName() );
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

	private void addAffectedEntities(AffectedEntities affectedEntities, NativeQueryImplementor<?> query) {
		for ( String space : affectedEntities.getAffectedSpaces( getFactory() ) ) {
			query.addSynchronizedQuerySpace( space );
		}
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

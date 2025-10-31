/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;


import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.graph.RootGraph;
import org.hibernate.jpa.internal.util.LockModeTypeHelper;
import org.hibernate.query.criteria.JpaCriteriaInsert;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.Identifier;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.query.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.session.ReactiveQueryProducer;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.Stage.MutationQuery;
import org.hibernate.reactive.stage.Stage.Query;
import org.hibernate.reactive.stage.Stage.SelectionQuery;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.hibernate.reactive.util.impl.CompletionStages.applyToAll;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Implements the {@link Stage.Session} API. This delegating class is
 * needed to avoid name clashes when implementing both
 * {@code Session} and {@link org.hibernate.Session}.
 */
public class StageSessionImpl implements Stage.Session {

	private final ReactiveSession delegate;

	public StageSessionImpl(ReactiveSession session) {
		this.delegate = session;
	}
	@Override
	public CompletionStage<Void> flush() {
//		checkOpen();
		return delegate.reactiveFlush();
	}

	@Override
	public <T> CompletionStage<T> fetch(T association) {
		return delegate.reactiveFetch( association, false );
	}

	@Override
	public <E,T> CompletionStage<T> fetch(E entity, Attribute<E,T> field) {
		return delegate.reactiveFetch( entity, field );
	}

	@Override
	public <T> CompletionStage<T> unproxy(T association) {
		return delegate.reactiveFetch( association, true );
	}

	public ReactiveConnection getReactiveConnection() {
		return delegate.getReactiveConnection();
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return delegate.getReference( entityClass, id );
	}

	@Override
	public <T> T getReference(T entity) {
		return delegate.getReference( delegate.getEntityClass(entity), delegate.getEntityId(entity) );
	}

	@Override
	public LockMode getLockMode(Object entity) {
		return delegate.getCurrentLockMode( entity );
	}

	@Override
	public boolean contains(Object entity) {
		return delegate.contains( entity );
	}

	@Override
	public <R> SelectionQuery<R> createSelectionQuery(String queryString, Class<R> resultType) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveSelectionQuery( queryString, resultType ) );
	}

	@Override
	public MutationQuery createMutationQuery(String queryString) {
		return new StageMutationQueryImpl<>( delegate.createReactiveMutationQuery( queryString ) );
	}

	@Override
	public MutationQuery createMutationQuery(CriteriaUpdate<?> updateQuery) {
		return new StageMutationQueryImpl<>( delegate.createReactiveMutationQuery( updateQuery ) );
	}

	@Override
	public MutationQuery createMutationQuery(CriteriaDelete<?> deleteQuery) {
		return new StageMutationQueryImpl<>( delegate.createReactiveMutationQuery( deleteQuery ) );
	}

	@Override
	public MutationQuery createMutationQuery(JpaCriteriaInsert<?> insert) {
		return new StageMutationQueryImpl<>( delegate.createReactiveMutationQuery( insert ) );
	}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey) {
		return delegate.reactiveFind( entityClass, primaryKey );
	}

	@Override
	public <T> CompletionStage<List<T>> find(Class<T> entityClass, Object... ids) {
		return delegate.reactiveFind( entityClass, ids );
	}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Identifier<T> id) {
		return delegate.reactiveFind( entityClass, id.namedValues() );
	}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey, LockMode lockMode) {
		return delegate.reactiveFind( entityClass, primaryKey, lockMode, null );
	}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object id, LockModeType lockModeType) {
		return find( entityClass, id, LockModeTypeHelper.getLockMode( lockModeType ) );
	}

	// FIXME: Should I delete this?
//	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey, LockOptions lockOptions) {
		return delegate.reactiveFind( entityClass, primaryKey, lockOptions, null );
	}

	@Override
	public <T> CompletionStage<T> find(EntityGraph<T> entityGraph, Object id) {
		Class<T> entityClass = ( (RootGraph<T>) entityGraph ).getGraphedType().getJavaType();
		return delegate.reactiveFind( entityClass, id, (LockOptions) null, entityGraph );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		return delegate.reactivePersist( entity );
	}

	@Override
	public CompletionStage<Void> persist(String entityName, Object entity) {
		return delegate.reactivePersist( entityName, entity );
	}

	@Override
	public CompletionStage<Void> persist(Object... entity) {
		return applyToAll( delegate::reactivePersist, entity );
	}

	@Override
	public CompletionStage<Void> remove(Object entity) {
		return delegate.reactiveRemove( entity );
	}

	@Override
	public CompletionStage<Void> remove(Object... entity) {
		return applyToAll( delegate::reactiveRemove, entity );
	}

	@Override
	public <T> CompletionStage<T> merge(T entity) {
		return delegate.reactiveMerge( entity );
	}

	@Override
	public final CompletionStage<Void> merge(Object... entity) {
		return applyToAll( delegate::reactiveMerge, entity );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity) {
		return delegate.reactiveRefresh( entity, LockOptions.NONE );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity, LockMode lockMode) {
		return delegate.reactiveRefresh( entity, lockMode );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity, LockModeType lockModeType) {
		return Stage.Session.super.refresh( entity, lockModeType );
	}

	//	@Override
	public CompletionStage<Void> refresh(Object entity, LockOptions lockOptions) {
		return delegate.reactiveRefresh( entity, lockOptions );
	}

	@Override
	public CompletionStage<Void> refresh(Object... entity) {
		return applyToAll( e -> delegate.reactiveRefresh( e, LockOptions.NONE ), entity );
	}

	@Override
	public CompletionStage<Void> lock(Object entity, LockMode lockMode) {
		return delegate.reactiveLock( entity, lockMode );
	}

	@Override
	public CompletionStage<Void> lock(Object entity, LockModeType lockModeType) {
		return Stage.Session.super.lock( entity, lockModeType );
	}

	// @Override
	public CompletionStage<Void> lock(Object entity, LockOptions lockOptions) {
		return delegate.reactiveLock( entity, lockOptions );
	}

	@Override
	public FlushMode getFlushMode() {
        return switch ( delegate.getHibernateFlushMode() ) {
            case MANUAL -> FlushMode.MANUAL;
            case COMMIT -> FlushMode.COMMIT;
            case AUTO -> FlushMode.AUTO;
            case ALWAYS -> FlushMode.ALWAYS;
        };
	}

	@Override
	public Stage.Session setFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode( switch ( flushMode ) {
			case COMMIT -> org.hibernate.FlushMode.COMMIT;
			case AUTO -> org.hibernate.FlushMode.AUTO;
			case MANUAL -> org.hibernate.FlushMode.MANUAL;
			case ALWAYS -> org.hibernate.FlushMode.ALWAYS;
		} );
		return this;
	}

	@Override
	public Stage.Session setFlushMode(FlushModeType flushModeType) {
		return Stage.Session.super.setFlushMode( flushModeType );
	}

	@Override
	public Stage.Session setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly(readOnly);
		return this;
	}

	@Override
	public boolean isDefaultReadOnly() {
		return delegate.isDefaultReadOnly();
	}

	@Override
	public Stage.Session setReadOnly(Object entityOrProxy, boolean readOnly) {
		delegate.setReadOnly(entityOrProxy, readOnly);
		return this;
	}

	@Override
	public boolean isReadOnly(Object entityOrProxy) {
		return delegate.isReadOnly(entityOrProxy);
	}

	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	public Stage.Session setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode(cacheMode);
		return this;
	}

	@Override
	public Stage.Session setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		return Stage.Session.super.setCacheStoreMode( cacheStoreMode );
	}

	@Override
	public Stage.Session setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		return Stage.Session.super.setCacheRetrieveMode( cacheRetrieveMode );
	}

	@Override
	public Stage.Session setBatchSize(Integer batchSize) {
		delegate.setBatchSize(batchSize);
		return this;
	}

	@Override
	public Integer getBatchSize() {
		return delegate.getBatchSize();
	}

	@Override
	public Stage.Session detach(Object entity) {
		delegate.detach(entity);
		return this;
	}

	@Override
	public Stage.Session clear() {
		delegate.clear();
		return this;
	}

	@Override
	public Stage.Session enableFetchProfile(String name) {
		delegate.enableFetchProfile(name);
		return this;
	}

	@Override
	public Stage.Session disableFetchProfile(String name) {
		delegate.disableFetchProfile(name);
		return this;
	}

	@Override
	public boolean isFetchProfileEnabled(String name) {
		return delegate.isFetchProfileEnabled(name);
	}

	@Override
	public Filter enableFilter(String filterName) {
		return delegate.enableFilter( filterName );
	}

	@Override
	public void disableFilter(String filterName) {
		delegate.disableFilter( filterName );
	}

	@Override
	public Filter getEnabledFilter(String filterName) {
		return delegate.getEnabledFilter( filterName );
	}

	@Override
	public int getFetchBatchSize() {
		return delegate.getFetchBatchSize();
	}

	@Override
	public Stage.Session setFetchBatchSize(int batchSize) {
		delegate.setFetchBatchSize( batchSize );
		return this;
	}

	@Override
	public boolean isSubselectFetchingEnabled() {
		return delegate.isSubselectFetchingEnabled();
	}

	@Override
	public Stage.Session setSubselectFetchingEnabled(boolean enabled) {
		delegate.setSubselectFetchingEnabled( enabled );
		return this;
	}

	@Override
	public <T> CompletionStage<T> withTransaction(Function<Stage.Transaction, CompletionStage<T>> work) {
		return currentTransaction == null ? new Transaction<T>().execute( work ) : work.apply( currentTransaction );
	}

	private Transaction<?> currentTransaction;

	@Override
	public Stage.Transaction currentTransaction() {
		return currentTransaction;
	}

	private class Transaction<T> implements Stage.Transaction {
		boolean rollback;
		Throwable error;

		CompletionStage<T> execute(Function<Stage.Transaction, CompletionStage<T>> work) {
			currentTransaction = this;
			return begin()
					.thenCompose( v -> executeInTransaction( work ) )
					.whenComplete( (t, x) -> currentTransaction = null );
		}

		/**
		 * Run the code assuming that a transaction has already started so that we can
		 * differentiate an error starting a transaction (and therefore doesn't need to
		 * roll back) and an error thrown by the work.
		 */
		CompletionStage<T> executeInTransaction(Function<Stage.Transaction, CompletionStage<T>> work) {
			return voidFuture().thenCompose( v -> work.apply( this ) )
					// only flush() if the work completed with no exception
					.thenCompose( result -> flush().thenApply( v -> result ) )
					// have to capture the error here and pass it along,
					// since we can't just return a CompletionStage that
					// rolls back the transaction from the handle() function
					.handle( this::processError )
					// finally, commit or roll back the transaction, and
					// then rethrow the caught error if necessary
					.thenCompose( result -> end()
							// make sure that if rollback() throws,
							// the original error doesn't get swallowed
							.handle( this::processError )
							// finally, rethrow the original error, if any
							.thenApply( v -> returnOrRethrow( error, result ) )
					);

		}

		CompletionStage<Void> flush() {
			return delegate.reactiveAutoflush();
		}

		CompletionStage<Void> begin() {
			return delegate.getReactiveConnection().beginTransaction();
		}

		CompletionStage<Void> end() {
			ReactiveActionQueue actionQueue = delegate.getReactiveActionQueue();
			return actionQueue.beforeTransactionCompletion()
					.thenApply( v -> delegate.getReactiveConnection() )
					.thenCompose( c -> rollback ? c.rollbackTransaction() : c.commitTransaction() )
					.thenCompose( v -> actionQueue.afterTransactionCompletion( !rollback ) );
		}

		<R> R processError(R result, Throwable e) {
			if ( e!=null ) {
				rollback = true;
				if (error == null) {
					error = e;
				}
				else {
					error.addSuppressed(e);
				}
			}
			return result;
		}

		@Override
		public void markForRollback() {
			rollback = true;
		}

		@Override
		public boolean isMarkedForRollback() {
			return rollback;
		}
	}

	@Override
	public CompletionStage<Void> close() {
		return delegate.reactiveClose();
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public Stage.SessionFactory getFactory() {
		return delegate.getFactory().unwrap( Stage.SessionFactory.class );
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		return getFactory().getCriteriaBuilder();
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		return delegate.getResultSetMapping( resultType, mappingName );
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName) {
		return delegate.getEntityGraph( rootType, graphName );
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
		return delegate.createEntityGraph( rootType );
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName) {
		return delegate.createEntityGraph( rootType, graphName );
	}

	@Override
	public <R> Query<R> createQuery(TypedQueryReference<R> typedQueryReference) {
		ReactiveQuery<R> reactiveQuery = delegate.createReactiveQuery( typedQueryReference );
		return new StageQueryImpl<>( reactiveQuery );
	}

	@Override @Deprecated
	public <R> Query<R> createQuery(String queryString) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( queryString ) );
	}

	@Override
	public <R> SelectionQuery<R> createQuery(String queryString, Class<R> resultType) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveQuery( queryString, resultType ) );
	}

	@Override
	public <R> MutationQuery createQuery(CriteriaUpdate<R> criteriaUpdate) {
		return new StageMutationQueryImpl<>( delegate.createReactiveMutationQuery( criteriaUpdate ) );
	}

	@Override
	public <R> SelectionQuery<R> createQuery(CriteriaQuery<R> criteriaQuery) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveQuery( criteriaQuery ) );
	}

	@Override
	public <R> MutationQuery createQuery(CriteriaDelete<R> criteriaDelete) {
		return new StageMutationQueryImpl<>( delegate.createReactiveMutationQuery( criteriaDelete ) );
	}

	@Override
	public <R> Query<R> createNamedQuery(String queryName) {
		return new StageQueryImpl<>( delegate.createReactiveNamedQuery( queryName ) );
	}

	@Override
	public <R> SelectionQuery<R> createNamedQuery(String queryName, Class<R> resultType) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveNamedQuery( queryName, resultType ) );
	}

	@Override
	public <R> SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultType ) );
	}

	@Override
	public <R> SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultType, affectedEntities ) );
	}

	@Override
	public <R> SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping ) );
	}

	@Override
	public <R> SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities) {
		return new StageSelectionQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping, affectedEntities ) );
	}

	@Override
	public <R> Query<R> createNativeQuery(String queryString) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery( queryString ) );
	}

	@Override
	public <R> Query<R> createNativeQuery(String queryString, AffectedEntities affectedEntities) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery( queryString, affectedEntities ) );
	}

	public <T> T unwrap(Class<T> clazz) {
		if ( ReactiveConnectionSupplier.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( this.delegate );
		}
		else if ( ReactiveSession.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( this.delegate );
		}
		else if ( ReactiveQueryProducer.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( this.delegate );
		}
		else if ( ReactiveSharedSessionContractImplementor.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( this.delegate );
		}
		throw new PersistenceException( "Cannot unwrap type " + clazz );
	}
}

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
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.Identifier;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.hibernate.reactive.util.impl.CompletionStages.applyToAll;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * Implements the {@link Stage.Session} API. This delegating class is
 * needed to avoid name clashes when implementing both
 * {@code Session} and {@link org.hibernate.Session}.
 */
public class StageSessionImpl implements Stage.Session {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

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
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey) {
		return delegate.reactiveFind( entityClass, primaryKey, null, null );
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
		return delegate.reactiveFind( entityClass, primaryKey, new LockOptions( lockMode ), null );
	}

//	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey, LockOptions lockOptions) {
		return delegate.reactiveFind( entityClass, primaryKey, lockOptions, null );
	}

	@Override
	public <T> CompletionStage<T> find(EntityGraph<T> entityGraph, Object id) {
		Class<T> entityClass = ((RootGraphImplementor<T>) entityGraph).getGraphedType().getJavaType();
		return delegate.reactiveFind( entityClass, id, null, entityGraph );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		return delegate.reactivePersist( entity );
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

	@Override @SafeVarargs
	public final <T> CompletionStage<Void> merge(T... entity) {
		return applyToAll( delegate::reactiveMerge, entity );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity) {
		return delegate.reactiveRefresh( entity, LockOptions.NONE );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity, LockMode lockMode) {
		return delegate.reactiveRefresh( entity, new LockOptions(lockMode) );
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
		return delegate.reactiveLock( entity, new LockOptions(lockMode) );
	}

//	@Override
	public CompletionStage<Void> lock(Object entity, LockOptions lockOptions) {
		return delegate.reactiveLock( entity, lockOptions );
	}

	@Override
	public <R> Stage.Query<R> createQuery(String jpql) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( jpql ) );
	}

	@Override
	public <R> Stage.Query<R> createQuery(String jpql, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( jpql, resultType ) );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String sql) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery( sql ) );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String sql, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery(sql, resultType) );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String sql, ResultSetMapping<R> resultSetMapping) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery( sql, resultSetMapping.getName() ) );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String sql, Class<R> resultType, AffectedEntities affectedEntities) {
		return new StageQueryImpl<>(
				delegate.createReactiveNativeQuery( sql, resultType ),
				affectedEntities.getAffectedSpaces( delegate.getFactory() )
		);
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String sql, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities) {
		return new StageQueryImpl<>(
				delegate.createReactiveNativeQuery( sql, resultSetMapping.getName() ),
				affectedEntities.getAffectedSpaces( delegate.getFactory() )
		);
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String sql, AffectedEntities affectedEntities) {
		return new StageQueryImpl<>(
				delegate.createReactiveNativeQuery( sql ),
				affectedEntities.getAffectedSpaces( delegate.getFactory() )
		);
	}

	@Override
	public <R> Stage.Query<R> createNamedQuery(String name) {
		return new StageQueryImpl<>( delegate.createReactiveNamedQuery( name ) );
	}

	@Override
	public <R> Stage.Query<R> createNamedQuery(String name, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveNamedQuery( name, resultType ) );
	}

	@Override @SuppressWarnings("unchecked")
	public <R> Stage.Query<R> createQuery(CriteriaQuery<R> criteriaQuery) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaQuery ) );
	}

	@Override @SuppressWarnings("unchecked")
	public <R> Stage.Query<R> createQuery(CriteriaUpdate<R> criteriaUpdate) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaUpdate ) );
	}

	@Override @SuppressWarnings("unchecked")
	public <R> Stage.Query<R> createQuery(CriteriaDelete<R> criteriaDelete) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaDelete ) );
	}

	@Override
	public FlushMode getFlushMode() {
		switch ( delegate.getHibernateFlushMode() ) {
			case MANUAL:
				return FlushMode.MANUAL;
			case COMMIT:
				return FlushMode.COMMIT;
			case AUTO:
				return FlushMode.AUTO;
			case ALWAYS:
				return FlushMode.ALWAYS;
			default:
				throw LOG.impossibleFlushModeIllegalState();
		}
	}

	@Override
	public Stage.Session setFlushMode(FlushMode flushMode) {
		switch (flushMode) {
			case COMMIT:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.COMMIT);
				break;
			case AUTO:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.AUTO);
				break;
			case MANUAL:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.MANUAL);
				break;
			case ALWAYS:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.ALWAYS);
				break;
		}
		return this;
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
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		return delegate.getResultSetMapping(resultType, mappingName);
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> entity, String name) {
		return delegate.getEntityGraph(entity, name);
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity) {
		return delegate.createEntityGraph(entity);
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity, String name) {
		return delegate.createEntityGraph(entity, name);
	}

	@Override
	public Filter enableFilter(String filterName) {
		return delegate.enableFilter(filterName);
	}

	@Override
	public void disableFilter(String filterName) {
		delegate.disableFilter(filterName);
	}

	@Override
	public Filter getEnabledFilter(String filterName) {
		return delegate.getEnabledFilter(filterName);
	}

	@Override
	public <T> CompletionStage<T> withTransaction(Function<Stage.Transaction, CompletionStage<T>> work) {
		return currentTransaction==null ? new Transaction<T>().execute(work) : work.apply(currentTransaction);
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
		 * differentiate an error starting a transaction (and therefore doesn't need to rollback)
		 * and an error thrown by the work.
		 */
		CompletionStage<T> executeInTransaction(Function<Stage.Transaction, CompletionStage<T>> work) {
			return work.apply( this )
					// only flush() if the work completed with no exception
					.thenCompose( result -> flush().thenApply( v -> result ) )
					// have to capture the error here and pass it along,
					// since we can't just return a CompletionStage that
					// rolls back the transaction from the handle() function
					.handle( this::processError )
					// finally, commit or rollback the transaction, and
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

}

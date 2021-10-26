/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import org.hibernate.LockMode;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.ReactiveStatelessSession;
import org.hibernate.reactive.stage.Stage;

import javax.persistence.EntityGraph;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * Implements the {@link Stage.StatelessSession} API. This delegating
 * class is needed to avoid name clashes when implementing both
 * {@code StatelessSession} and {@link org.hibernate.StatelessSession}.
 */
public class StageStatelessSessionImpl implements Stage.StatelessSession {

	private final ReactiveStatelessSession delegate;
	private final StageSessionFactoryImpl factory;

	public StageStatelessSessionImpl(ReactiveStatelessSession delegate, StageSessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	private <T> CompletionStage<T> stage(Function<Void, CompletionStage<T>> stage) {
		return factory.stage( stage );
	}

	@Override
	public <T> CompletionStage<T> get(Class<T> entityClass, Object id) {
		return stage( v -> delegate.reactiveGet( entityClass, id ) );
	}

	@Override
	public <T> CompletionStage<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
		return stage( v -> delegate.reactiveGet( entityClass, id, lockMode, null ) );
	}

	@Override
	public <T> CompletionStage<T> get(EntityGraph<T> entityGraph, Object id) {
		Class<T> entityClass = ( (RootGraphImplementor<T>) entityGraph ).getGraphedType().getJavaType();
		return stage( v -> delegate.reactiveGet( entityClass, id, null, entityGraph ) );
	}

	@Override
	public <R> Stage.Query<R> createQuery(String queryString) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( queryString ), factory );
	}

	@Override
	public <R> Stage.Query<R> createQuery(String queryString, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( queryString, resultType ), factory );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String queryString) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery( queryString ), factory );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultType ), factory );
	}

	@Override
	public <R> Stage.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> sqlResultSetMapping) {
		return new StageQueryImpl<>(
				delegate.createReactiveNativeQuery( queryString, sqlResultSetMapping.getName() ),
				factory
		);
	}

	@Override
	public <R> Stage.Query<R> createNamedQuery(String name) {
		return new StageQueryImpl<>( delegate.createReactiveNamedQuery( name ), factory );
	}

	@Override
	public <R> Stage.Query<R> createNamedQuery(String name, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveNamedQuery( name, resultType ), factory );
	}

	@Override
	public <R> Stage.Query<R> createQuery(CriteriaQuery<R> criteriaQuery) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaQuery ), factory );
	}

	@Override
	public <R> Stage.Query<R> createQuery(CriteriaUpdate<R> criteriaUpdate) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaUpdate ), factory );
	}

	@Override
	public <R> Stage.Query<R> createQuery(CriteriaDelete<R> criteriaDelete) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaDelete ), factory );
	}

	@Override
	public CompletionStage<Void> insert(Object entity) {
		return stage( w -> delegate.reactiveInsert( entity ) );
	}

	@Override
	public CompletionStage<Void> insert(Object... entities) {
		return stage( w -> delegate.reactiveInsertAll( entities ) );
	}

	@Override
	public CompletionStage<Void> delete(Object entity) {
		return stage( w -> delegate.reactiveDelete( entity ) );
	}

	@Override
	public CompletionStage<Void> delete(Object... entities) {
		return stage( w -> delegate.reactiveDeleteAll( entities ) );
	}

	@Override
	public CompletionStage<Void> update(Object entity) {
		return stage( w -> delegate.reactiveUpdate( entity ) );
	}

	@Override
	public CompletionStage<Void> update(Object... entities) {
		return stage( w -> delegate.reactiveUpdateAll( entities ) );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity) {
		return stage( w -> delegate.reactiveRefresh( entity ) );
	}

	@Override
	public CompletionStage<Void> refresh(Object... entities) {
		return stage( w -> delegate.reactiveRefreshAll( entities ) );
	}

	@Override
	public CompletionStage<Void> refresh(Object entity, LockMode lockMode) {
		return stage( w -> delegate.reactiveRefresh( entity, lockMode ) );
	}

	@Override
	public <T> CompletionStage<T> fetch(T association) {
		return stage( w -> delegate.reactiveFetch( association, false ) );
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		return delegate.getResultSetMapping( resultType, mappingName );
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> entity, String name) {
		return delegate.getEntityGraph( entity, name );
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity) {
		return delegate.createEntityGraph( entity );
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity, String name) {
		return delegate.createEntityGraph( entity, name );
	}

	@Override
	public <T> CompletionStage<T> withTransaction(Function<Stage.Transaction, CompletionStage<T>> work) {
		return currentTransaction == null ? new Transaction<T>().execute( work ) : work.apply( currentTransaction );
	}

	@Override
	public CompletionStage<Void> close() {
		return stage( v -> {
			CompletableFuture<Void> closing = new CompletableFuture<>();
			delegate.close( closing );
			return closing;
		} );
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
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
					.thenCompose( v -> work.apply( this ) )
					// have to capture the error here and pass it along,
					// since we can't just return a CompletionStage that
					// rolls back the transaction from the handle() function
					.handle( this::processError )
					// finally, commit or rollback the transaction, and
					// then rethrow the caught error if necessary
					.thenCompose(
							result -> end()
									// make sure that if rollback() throws,
									// the original error doesn't get swallowed
									.handle( this::processError )
									// finally rethrow the original error, if any
									.thenApply( v -> returnOrRethrow( error, result ) )
					)
					.whenComplete( (t, x) -> currentTransaction = null );
		}

		CompletionStage<Void> begin() {
			return delegate.getReactiveConnection().beginTransaction();
		}

		CompletionStage<Void> end() {
			ReactiveConnection c = delegate.getReactiveConnection();
			return rollback ? c.rollbackTransaction() : c.commitTransaction();
		}

		<R> R processError(R result, Throwable e) {
			if ( e != null ) {
				rollback = true;
				if ( error == null ) {
					error = e;
				}
				else {
					error.addSuppressed( e );
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
}

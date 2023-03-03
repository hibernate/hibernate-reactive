/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import jakarta.persistence.criteria.CriteriaQuery;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.LockMode;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveStatelessSession;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.EntityGraph;


/**
 * Implements the {@link Mutiny.StatelessSession} API. This delegating
 * class is needed to avoid name clashes when implementing both
 * {@code StatelessSession} and {@link org.hibernate.StatelessSession}.
 */
public class MutinyStatelessSessionImpl implements Mutiny.StatelessSession {

	private final ReactiveStatelessSession delegate;
	private final MutinySessionFactoryImpl factory;

	public MutinyStatelessSessionImpl(ReactiveStatelessSession delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	public ReactiveConnection getReactiveConnection() {
		return delegate.getReactiveConnection();
	}


	<T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public <T> Uni<T> get(Class<T> entityClass, Object id) {
		return uni( () -> delegate.reactiveGet( entityClass, id ) );
	}

	@Override
	public <T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
		return uni( () -> delegate.reactiveGet( entityClass, id, lockMode, null ) );
	}

	@Override
	public <T> Uni<T> get(EntityGraph<T> entityGraph, Object id) {
		Class<T> entityClass = ( (RootGraphImplementor<T>) entityGraph ).getGraphedType().getJavaType();
		return uni( () -> delegate.reactiveGet( entityClass, id, null, entityGraph ) );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(String queryString) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( queryString ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(String queryString, Class<R> resultType) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( queryString, resultType ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createNamedQuery(String queryName) {
		return new MutinyQueryImpl<>( delegate.createReactiveNamedQuery( queryName, null ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createNamedQuery(String queryName, Class<R> resultType) {
		return new MutinyQueryImpl<>( delegate.createReactiveNamedQuery( queryName, resultType ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createNativeQuery(String queryString) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultType ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(CriteriaQuery<R> criteriaQuery) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( criteriaQuery ), factory );
	}

	@Override
	public Uni<Void> insert(Object entity) {
		return uni( () -> delegate.reactiveInsert( entity ) );
	}

	@Override
	public Uni<Void> insertAll(Object... entities) {
		return uni( () -> delegate.reactiveInsertAll( entities ) );
	}

	@Override
	public Uni<Void> insertAll(int batchSize, Object... entities) {
		return uni( () -> delegate.reactiveInsertAll( batchSize, entities ) );
	}

	@Override
	public Uni<Void> delete(Object entity) {
		return uni( () -> delegate.reactiveDelete( entity ) );
	}

	@Override
	public Uni<Void> deleteAll(Object... entities) {
		return uni( () -> delegate.reactiveDeleteAll( entities ) );
	}

	@Override
	public Uni<Void> deleteAll(int batchSize, Object... entities) {
		return uni( () -> delegate.reactiveDeleteAll( entities ) );
	}

	@Override
	public Uni<Void> update(Object entity) {
		return uni( () -> delegate.reactiveUpdate( entity ) );
	}

	@Override
	public Uni<Void> updateAll(Object... entities) {
		return uni( () -> delegate.reactiveUpdateAll( entities ) );
	}

	@Override
	public Uni<Void> updateAll(int batchSize, Object... entities) {
		return uni( () -> delegate.reactiveUpdateAll( batchSize, entities ) );
	}

	@Override
	public Uni<Void> refresh(Object entity) {
		return uni( () -> delegate.reactiveRefresh( entity ) );
	}

	@Override
	public Uni<Void> refreshAll(Object... entities) {
		return uni( () -> delegate.reactiveRefreshAll( entities ) );
	}

	@Override
	public Uni<Void> refreshAll(int batchSize, Object... entities) {
		return uni( () -> delegate.reactiveRefreshAll( batchSize, entities ) );
	}

	@Override
	public Uni<Void> refresh(Object entity, LockMode lockMode) {
		return uni( () -> delegate.reactiveRefresh( entity, lockMode ) );
	}

	@Override
	public <T> Uni<T> fetch(T association) {
		return uni( () -> delegate.reactiveFetch( association, false ) );
	}

//	@Override
//	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
//		return delegate.getResultSetMapping( resultType, mappingName );
//	}
//
//	@Override
//	public <T> EntityGraph<T> getEntityGraph(Class<T> entity, String name) {
//		return delegate.getEntityGraph( entity, name );
//	}
//
//	@Override
//	public <T> EntityGraph<T> createEntityGraph(Class<T> entity) {
//		return delegate.createEntityGraph( entity );
//	}
//
//	@Override
//	public <T> EntityGraph<T> createEntityGraph(Class<T> entity, String name) {
//		return delegate.createEntityGraph( entity, name );
//	}

	@Override
	public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
		return currentTransaction == null ? new Transaction<T>().execute( work ) : work.apply( currentTransaction );
	}

	private Transaction<?> currentTransaction;

	@Override
	public Mutiny.Transaction currentTransaction() {
		return currentTransaction;
	}

	private class Transaction<T> implements Mutiny.Transaction {
		boolean rollback;

		Uni<T> execute(Function<Mutiny.Transaction, Uni<T>> work) {
			currentTransaction = this;
			return begin()
					.chain( () -> executeInTransaction( work ) )
					.eventually( () -> currentTransaction = null );
		}

		/**
		 * Run the code assuming that a transaction has already started so that we can
		 * differentiate an error starting a transaction (and therefore doesn't need to rollback)
		 * and an error thrown by the work.
		 */
		Uni<T> executeInTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
			return work.apply( this )
					// in the case of an exception or cancellation
					// we need to rollback the transaction
					.onFailure().call( this::rollback )
					.onCancellation().call( this::rollback )
					// finally, when there was no exception,
					// commit or rollback the transaction
					.call( () -> rollback ? rollback() : commit() );
		}

		Uni<Void> begin() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().beginTransaction() );
		}

		Uni<Void> rollback() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().rollbackTransaction() );
		}

		Uni<Void> commit() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().commitTransaction() );
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
	public Uni<Void> close() {
		return uni( () -> {
			CompletableFuture<Void> closing = new CompletableFuture<>();
			delegate.close( closing );
			return closing;
		} );
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName) {
		throw new UnsupportedOperationException("Not yet implemented");
	}
}

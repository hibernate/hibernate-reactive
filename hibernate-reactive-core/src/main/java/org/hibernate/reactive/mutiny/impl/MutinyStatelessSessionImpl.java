/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import org.hibernate.LockMode;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.ReactiveStatelessSession;

import javax.persistence.EntityGraph;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

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
        Vertx.currentContext().putLocal( Mutiny.StatelessSession.class.getName(), this );
    }

    <T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
        return factory.uni(stageSupplier);
    }

    @Override
    public <T> Uni<T> get(Class<T> entityClass, Object id) {
        return uni( () -> delegate.reactiveGet(entityClass, id) );
    }

    @Override
    public <T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return uni( () -> delegate.reactiveGet(entityClass, id, lockMode, null) );
    }

    @Override
    public <T> Uni<T> get(EntityGraph<T> entityGraph, Object id) {
        Class<T> entityClass = ((RootGraphImplementor<T>) entityGraph).getGraphedType().getJavaType();
        return uni( () -> delegate.reactiveGet(entityClass, id, null, entityGraph) );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(String queryString) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery(queryString), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(String queryString, Class<R> resultType) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery(queryString, resultType), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
        boolean knownType = delegate.getFactory().getMetamodel()
                .entityPersisters().containsKey( resultType.getName() );
        return knownType
                ? new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString, resultType), factory )
                : new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping.getName() ), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(CriteriaQuery<R> criteriaQuery) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaQuery ), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(CriteriaUpdate<R> criteriaUpdate) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaUpdate ), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(CriteriaDelete<R> criteriaDelete) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaDelete ), factory );
    }

    @Override
    public Uni<Void> insert(Object entity) {
        return uni( () -> delegate.reactiveInsert(entity) );
    }

    @Override
    public Uni<Void> insertAll(Object... entities) {
        return uni( () -> delegate.reactiveInsertAll(entities) );
    }

    @Override
    public Uni<Void> delete(Object entity) {
        return uni( () -> delegate.reactiveDelete(entity) );
    }

    @Override
    public Uni<Void> deleteAll(Object... entities) {
        return uni( () -> delegate.reactiveDeleteAll(entities) );
    }

    @Override
    public Uni<Void> update(Object entity) {
        return uni( () -> delegate.reactiveUpdate(entity) );
    }

    @Override
    public Uni<Void> updateAll(Object... entities) {
        return uni( () -> delegate.reactiveUpdateAll(entities) );
    }

    @Override
    public Uni<Void> refresh(Object entity) {
        return uni( () -> delegate.reactiveRefresh(entity) );
    }

    @Override
    public Uni<Void> refreshAll(Object... entities) {
        return uni( () -> delegate.reactiveRefreshAll(entities) );
    }

    @Override
    public Uni<Void> refresh(Object entity, LockMode lockMode) {
        return uni( () -> delegate.reactiveRefresh(entity, lockMode) );
    }

    @Override
    public <T> Uni<T> fetch(T association) {
        return uni( () -> delegate.reactiveFetch(association, false) );
    }

    @Override
    public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
        return delegate.getResultSetMapping( resultType, mappingName );
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
    public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
        return currentTransaction==null ? new Transaction<T>().execute(work) : work.apply(currentTransaction);
    }

    private Transaction<?> currentTransaction;

    @Override
    public Mutiny.Transaction currentTransaction() {
        return currentTransaction;
    }

    private class Transaction<T> implements Mutiny.Transaction {
        boolean rollback;

        Uni<T> execute(Function<Mutiny.Transaction, Uni<T>> work) {
            //noinspection Convert2MethodRef
            return begin()
                    .chain( () -> work.apply( this ) )
                    // in the case of an exception or cancellation
                    // we need to rollback the transaction
                    .onFailure().call( () -> rollback() )
                    .onCancellation().call( () -> rollback() )
                    // finally, when there was no exception,
                    // commit or rollback the transaction
                    .onItem().call( () -> rollback ? rollback() : commit() )
                    .eventually( () -> cleanup() );
        }

        Uni<Void> begin() {
            currentTransaction = this;
            return Uni.createFrom().completionStage( delegate.getReactiveConnection().beginTransaction() );
        }

        Uni<Void> rollback() {
            return Uni.createFrom().completionStage( delegate.getReactiveConnection().rollbackTransaction() );
        }

        Uni<Void> commit() {
            return Uni.createFrom().completionStage( delegate.getReactiveConnection().commitTransaction() );
        }

        private void cleanup() {
            currentTransaction = null;
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
        Vertx.currentContext().removeLocal( Mutiny.StatelessSession.class.getName() );
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
}

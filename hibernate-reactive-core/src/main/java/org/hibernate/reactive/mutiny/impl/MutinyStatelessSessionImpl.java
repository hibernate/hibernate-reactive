/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.LockMode;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.session.ReactiveStatelessSession;

import javax.persistence.EntityGraph;
import java.util.concurrent.CompletionStage;
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
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString, resultType), factory );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping.getName() ), factory );
    }

    @Override
    public Uni<Void> insert(Object entity) {
        return uni( () -> delegate.reactiveInsert(entity) );
    }

    @Override
    public Uni<Void> delete(Object entity) {
        return uni( () -> delegate.reactiveDelete(entity) );
    }

    @Override
    public Uni<Void> update(Object entity) {
        return uni( () -> delegate.reactiveUpdate(entity) );
    }

    @Override
    public Uni<Void> refresh(Object entity) {
        return uni( () -> delegate.reactiveRefresh(entity) );
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
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }
}

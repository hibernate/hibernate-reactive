/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.LockMode;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.session.ReactiveStatelessSession;

/**
 * Implements the {@link Mutiny.StatelessSession} API. This delegating
 * class is needed to avoid name clashes when implementing both
 * {@code StatelessSession} and {@link org.hibernate.StatelessSession}.
 */
public class MutinyStatelessSessionImpl implements Mutiny.StatelessSession {

    private ReactiveStatelessSession delegate;

    public MutinyStatelessSessionImpl(ReactiveStatelessSession delegate) {
        this.delegate = delegate;
    }

    @Override
    public <T> Uni<T> get(Class<T> entityClass, Object id) {
        return Uni.createFrom().completionStage( delegate.reactiveGet(entityClass, id) );
    }

    @Override
    public <T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return Uni.createFrom().completionStage( delegate.reactiveGet(entityClass, id, lockMode) );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(String queryString) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery(queryString) );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(String queryString, Class<R> resultType) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery(queryString, resultType) );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString) );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString, resultType) );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping.getName() ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> insert(Object entity) {
        return Uni.createFrom().completionStage( delegate.reactiveInsert(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> delete(Object entity) {
        return Uni.createFrom().completionStage( delegate.reactiveDelete(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> update(Object entity) {
        return Uni.createFrom().completionStage( delegate.reactiveUpdate(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> refresh(Object entity) {
        return Uni.createFrom().completionStage( delegate.reactiveRefresh(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> refresh(Object entity, LockMode lockMode) {
        return Uni.createFrom().completionStage( delegate.reactiveRefresh(entity, lockMode).thenApply( v -> this ) );
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

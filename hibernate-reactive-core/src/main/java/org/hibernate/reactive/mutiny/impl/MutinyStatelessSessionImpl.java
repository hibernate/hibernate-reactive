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

    private final ReactiveStatelessSession delegate;
    private final MutinyUniConnectionActivator uni;

    public MutinyStatelessSessionImpl(ReactiveStatelessSession delegate) {
        this.delegate = delegate;
        this.uni = MutinyUniConnectionActivator.create( delegate.getReactiveConnection() );
    }

    @Override
    public <T> Uni<T> get(Class<T> entityClass, Object id) {
        return uni.asUni( delegate.reactiveGet(entityClass, id) );
    }

    @Override
    public <T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return uni.asUni( delegate.reactiveGet(entityClass, id, lockMode) );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(String queryString) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery(queryString), this.uni );
    }

    @Override
    public <R> Mutiny.Query<R> createQuery(String queryString, Class<R> resultType) {
        return new MutinyQueryImpl<>( delegate.createReactiveQuery(queryString, resultType), this.uni );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString), this.uni );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery(queryString, resultType), this.uni );
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping.getName() ), this.uni );
    }

    @Override
    public Uni<Mutiny.StatelessSession> insert(Object entity) {
        return uni.asUni( delegate.reactiveInsert(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> delete(Object entity) {
        return uni.asUni( delegate.reactiveDelete(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> update(Object entity) {
        return uni.asUni( delegate.reactiveUpdate(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> refresh(Object entity) {
        return uni.asUni( delegate.reactiveRefresh(entity).thenApply( v -> this ) );
    }

    @Override
    public Uni<Mutiny.StatelessSession> refresh(Object entity, LockMode lockMode) {
        return uni.asUni( delegate.reactiveRefresh(entity, lockMode).thenApply( v -> this ) );
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

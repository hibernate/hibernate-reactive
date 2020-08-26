/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import org.hibernate.LockMode;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.session.ReactiveStatelessSession;
import org.hibernate.reactive.stage.Stage;

import java.util.concurrent.CompletionStage;

/**
 * Implements the {@link Stage.StatelessSession} API. This delegating
 * class is needed to avoid name clashes when implementing both
 * {@code StatelessSession} and {@link org.hibernate.StatelessSession}.
 */
public class StageStatelessSessionImpl implements Stage.StatelessSession {

    private ReactiveStatelessSession delegate;

    public StageStatelessSessionImpl(ReactiveStatelessSession delegate) {
        this.delegate = delegate;
    }

    @Override
    public <T> CompletionStage<T> get(Class<T> entityClass, Object id) {
        return delegate.reactiveGet(entityClass, id);
    }

    @Override
    public <T> CompletionStage<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return delegate.reactiveGet(entityClass, id, lockMode);
    }

    @Override
    public <R> Stage.Query<R> createQuery(String queryString) {
        return new StageQueryImpl<>( delegate.createReactiveQuery(queryString) );
    }

    @Override
    public <R> Stage.Query<R> createQuery(String queryString, Class<R> resultType) {
        return new StageQueryImpl<>( delegate.createReactiveQuery(queryString, resultType) );
    }

    @Override
    public <R> Stage.Query<R> createNativeQuery(String queryString) {
        return new StageQueryImpl<>( delegate.createReactiveNativeQuery(queryString) );
    }

    @Override
    public <R> Stage.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
        return new StageQueryImpl<>( delegate.createReactiveNativeQuery(queryString, resultType) );
    }

    @Override
    public <R> Stage.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> sqlResultSetMapping) {
        return new StageQueryImpl<>( delegate.createReactiveNativeQuery( queryString, sqlResultSetMapping.getName() ) );
    }

    @Override
    public CompletionStage<Stage.StatelessSession> insert(Object entity) {
        return delegate.reactiveInsert(entity).thenApply( v -> this );
    }

    @Override
    public CompletionStage<Stage.StatelessSession> delete(Object entity) {
        return delegate.reactiveDelete(entity).thenApply( v -> this );
    }

    @Override
    public CompletionStage<Stage.StatelessSession> update(Object entity) {
        return delegate.reactiveUpdate(entity).thenApply( v -> this );
    }

    @Override
    public CompletionStage<Stage.StatelessSession> refresh(Object entity) {
        return delegate.reactiveRefresh(entity).thenApply( v -> this );
    }

    @Override
    public CompletionStage<Stage.StatelessSession> refresh(Object entity, LockMode lockMode) {
        return delegate.reactiveRefresh(entity, lockMode).thenApply( v -> this );
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

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
import java.util.function.Function;

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
        return factory.stage(stage);
    }

    @Override
    public <T> CompletionStage<T> get(Class<T> entityClass, Object id) {
        return stage( v -> delegate.reactiveGet(entityClass, id) );
    }

    @Override
    public <T> CompletionStage<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return stage( v -> delegate.reactiveGet(entityClass, id, lockMode) );
    }

    @Override
    public <R> Stage.Query<R> createQuery(String queryString) {
        return new StageQueryImpl<>( delegate.createReactiveQuery(queryString), factory );
    }

    @Override
    public <R> Stage.Query<R> createQuery(String queryString, Class<R> resultType) {
        return new StageQueryImpl<>( delegate.createReactiveQuery(queryString, resultType), factory );
    }

    @Override
    public <R> Stage.Query<R> createNativeQuery(String queryString) {
        return new StageQueryImpl<>( delegate.createReactiveNativeQuery(queryString), factory );
    }

    @Override
    public <R> Stage.Query<R> createNativeQuery(String queryString, Class<R> resultType) {
        return new StageQueryImpl<>( delegate.createReactiveNativeQuery(queryString, resultType), factory );
    }

    @Override
    public <R> Stage.Query<R> createNativeQuery(String queryString, ResultSetMapping<R> sqlResultSetMapping) {
        return new StageQueryImpl<>( delegate.createReactiveNativeQuery( queryString, sqlResultSetMapping.getName() ), factory );
    }

    @Override
    public CompletionStage<Void> insert(Object entity) {
        return stage( w -> delegate.reactiveInsert(entity) );
    }

    @Override
    public CompletionStage<Void> delete(Object entity) {
        return stage( w -> delegate.reactiveDelete(entity) );
    }

    @Override
    public CompletionStage<Void> update(Object entity) {
        return stage( w -> delegate.reactiveUpdate(entity) );
    }

    @Override
    public CompletionStage<Void> refresh(Object entity) {
        return stage( w -> delegate.reactiveRefresh(entity) );
    }

    @Override
    public CompletionStage<Void> refresh(Object entity, LockMode lockMode) {
        return stage( w -> delegate.reactiveRefresh(entity, lockMode) );
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

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.reactive.pool.ReactiveConnection;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Helper class to deal with closed {@link ProxyConnection}s.
 * Provides static instances of the exception to throw, a {@link CompletableFuture}
 * completed with that exception and the {@link ReactiveConnection} implementation
 * that will only return this exceptionally completed future.
 */
final class ClosedReactiveConnection implements ReactiveConnection {

    static final ReactiveConnection INSTANCE = new ClosedReactiveConnection();

    private static final CompletableFuture<Object> closedConnectionFuture;
    private static final RuntimeException closedConnectionException;
    static {
        closedConnectionException = new ClosedConnectionException();

        closedConnectionFuture = new CompletableFuture<>();
        closedConnectionFuture.completeExceptionally(closedConnectionException);
    }

    static <X> CompletionStage<X> completionStage() {
        @SuppressWarnings("unchecked") CompletableFuture<X> f = (CompletableFuture<X>) closedConnectionFuture;
        return f;
    }

    static IllegalStateException failure() {
        return new IllegalStateException("The session resp. proxied-connection has been closed");
    }

    @Override
    public CompletionStage<Void> execute(String sql) {
        return completionStage();
    }

    @Override
    public CompletionStage<Void> executeOutsideTransaction(String sql) {
        return completionStage();
    }

    @Override
    public CompletionStage<Integer> update(String sql) {
        return completionStage();
    }

    @Override
    public CompletionStage<Integer> update(String sql, Object[] paramValues) {
        return completionStage();
    }

    @Override
    public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
        return completionStage();
    }

    @Override
    public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
        return completionStage();
    }

    @Override
    public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
        return completionStage();
    }

    @Override
    public CompletionStage<Result> select(String sql) {
        return completionStage();
    }

    @Override
    public CompletionStage<Result> select(String sql, Object[] paramValues) {
        return completionStage();
    }

    @Override
    public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
        return completionStage();
    }

    @Override
    public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
        return completionStage();
    }

    @Override
    public CompletionStage<Void> beginTransaction() {
        return completionStage();
    }

    @Override
    public CompletionStage<Void> commitTransaction() {
        return completionStage();
    }

    @Override
    public CompletionStage<Void> rollbackTransaction() {
        return completionStage();
    }

    @Override
    public CompletionStage<Void> executeBatch() {
        return completionStage();
    }

    @Override
    public void close() {
        // it's closed, we're good
    }

    @Override
    public CompletionStage<ReactiveConnection> openConnection() {
        return completionStage();
    }

    static final class ClosedConnectionException extends RuntimeException {
        ClosedConnectionException() {
            super("The session resp. proxied-connection has been closed", null, false, false);
        }
    }
}

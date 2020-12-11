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

final class ClosedReactiveConnection implements ReactiveConnection {

    static final ReactiveConnection INSTANCE = new ClosedReactiveConnection();

    private <X> CompletionStage<X> closedFailure() {
        CompletableFuture<X> f = new CompletableFuture<>();
        f.completeExceptionally(new IllegalStateException("The session resp. proxied-connection has been closed"));
        return f;
    }

    @Override
    public CompletionStage<Void> execute(String sql) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Void> executeOutsideTransaction(String sql) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Integer> update(String sql) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Integer> update(String sql, Object[] paramValues) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
        return closedFailure();
    }

    @Override
    public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Result> select(String sql) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Result> select(String sql, Object[] paramValues) {
        return closedFailure();
    }

    @Override
    public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
        return closedFailure();
    }

    @Override
    public CompletionStage<Void> beginTransaction() {
        return closedFailure();
    }

    @Override
    public CompletionStage<Void> commitTransaction() {
        return closedFailure();
    }

    @Override
    public CompletionStage<Void> rollbackTransaction() {
        return closedFailure();
    }

    @Override
    public CompletionStage<Void> executeBatch() {
        return closedFailure();
    }

    @Override
    public void close() {
        // it's closed, we're good
    }
}

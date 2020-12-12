/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Helper class for tests that want to guarantee that a database connection is available
 * at a certain point.
 */
public class ProxyConnectionTestHelper {
    public static CompletableFuture<Object> delayedProxyConnection() {
        CompletableFuture<Object> delay = new CompletableFuture<>();
        ProxyConnection.setNewProxyConnectionWrapperForTests( (pool, tenant) ->
                ProxyConnection.newInstanceForTests( new ReactiveConnectionPoolMock( pool::getConnection, delay ), tenant )
        );
        return delay;
    }

    public static void cleanupDelayedProxyConnection() {
        ProxyConnection.resetNewProxyConnectionWrapperForTests();
    }

    static class ReactiveConnectionPoolMock implements ReactiveConnectionPool {
        private final CompletableFuture<Object> delay;
        private final Supplier<CompletionStage<ReactiveConnection>> connSupplier;

        ReactiveConnectionPoolMock(Supplier<CompletionStage<ReactiveConnection>> connSupplier, CompletableFuture<Object> delay) {
            // empty
            this.connSupplier = connSupplier;
            this.delay = delay;
        }

        @Override
        public CompletionStage<ReactiveConnection> getConnection() {
            return delay.thenCompose( x -> connSupplier.get() );
        }

        @Override
        public CompletionStage<ReactiveConnection> getConnection(String tenantId) {
            throw new UnsupportedOperationException("not tested/exercised, so it's not implemented");
        }

        @Override
        public ReactiveConnection getProxyConnection() {
            throw new UnsupportedOperationException("not tested/exercised, so it's not implemented");
        }

        @Override
        public ReactiveConnection getProxyConnection(String tenantId) {
            throw new UnsupportedOperationException("not tested/exercised, so it's not implemented");
        }
    }
}

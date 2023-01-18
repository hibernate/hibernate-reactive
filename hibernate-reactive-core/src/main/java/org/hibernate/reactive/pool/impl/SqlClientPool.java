/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * A pool of reactive connections backed by a supplier of
 * Vert.x {@link Pool} instances.
 * <p>
 * The Vert.x notion of pool is not to be confused with the
 * traditional JDBC notion of a connection pool: there is a
 * fundamental difference as the Vert.x pool should not be
 * shared across threads or with other Vert.x contexts.
 * <p>
 * Therefore, the reactive {@code SessionFactory} doesn't
 * retain a single instance of {@link Pool}, but rather has
 * a supplier which produces a new {@code Pool} within each
 * context.
 *
 * @see DefaultSqlClientPool the default implementation
 * @see ExternalSqlClientPool the implementation used in Quarkus
 */
public abstract class SqlClientPool implements ReactiveConnectionPool {

	/**
	 * @return the underlying Vert.x {@link Pool} for the current context.
	 */
	protected abstract Pool getPool();

	/**
	 * @return a Hibernate {@link SqlStatementLogger} for logging SQL
	 *         statements as they are executed
	 */
	protected abstract SqlStatementLogger getSqlStatementLogger();

	/**
	 * Get a {@link Pool} for the specified tenant.
	 * <p>
	 * This is an unimplemented operation which must be overridden by
	 * subclasses which support multitenancy.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @throws UnsupportedOperationException if multitenancy is not supported
	 *
	 * @see ReactiveConnectionPool#getConnection(String)
	 */
	protected Pool getTenantPool(String tenantId) {
		throw new UnsupportedOperationException("multitenancy not supported by built-in SqlClientPool");
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection() {
		return getConnectionFromPool( getPool() );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection(String tenantId) {
		return getConnectionFromPool( getTenantPool( tenantId ) );
	}

    private CompletionStage<ReactiveConnection> getConnectionFromPool(Pool pool) {
		return completionStage(
				pool.getConnection().map( this::newConnection ),
				ReactiveConnection::close
		);
    }

	/**
	 * @param onCancellation invoke when converted {@link java.util.concurrent.CompletionStage} cancellation.
	 */
    private <T> CompletionStage<T> completionStage(Future<T> future, Consumer<T> onCancellation) {
		CompletableFuture<T> completableFuture = new CompletableFuture<>();
		future.onComplete( ar -> {
			if ( ar.succeeded() ) {
				if ( completableFuture.isCancelled() ) {
					onCancellation.accept( ar.result() );
				}
				completableFuture.complete( ar.result() );
			} else {
				completableFuture.completeExceptionally( ar.cause() );
			}
		});
		return completableFuture;
    }

	private ReactiveConnection newConnection(SqlConnection connection) {
		return new SqlClientConnection( connection, getPool(), getSqlStatementLogger() );
	}

	@Override
	public ReactiveConnection getProxyConnection() {
		return new ProxyConnection( this );
	}

	@Override
	public ReactiveConnection getProxyConnection(String tenantId) {
		return new ProxyConnection( this, tenantId );
	}

}

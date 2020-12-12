/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 *
 * @see DefaultSqlClientPool the default implementation
 * @see ExternalSqlClientPool the implementation used in Quarkus
 */
public abstract class SqlClientPool implements ReactiveConnectionPool {

	/**
	 * @return the underlying Vert.x {@link Pool}
	 */
	protected abstract Pool getPool();

	/**
	 * @return a Hibernate {@link SqlStatementLogger} for logging SQL
	 *         statements as they are executed
	 */
	protected abstract SqlStatementLogger getSqlStatementLogger();

	/**
	 * @return should `?`-style placeholders in the given SQL be
	 *         converted to `$n$` format for execution by the
	 *         PostgreSQL driver
	 */
	protected abstract boolean usePostgresStyleParameters();

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
	 * @see #getConnection(String)
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
		return Handlers.toCompletionStage(
				handler -> pool.getConnection(
						ar -> handler.handle(
								ar.succeeded()
										? succeededFuture( newConnection( ar.result() ) )
										: failedFuture( ar.cause() )
						)
				)
		);
	}

	private SqlClientConnection newConnection(SqlConnection connection) {
		return new SqlClientConnection( connection, getPool(), getSqlStatementLogger(), usePostgresStyleParameters() );
	}

	@Override
	public ReactiveConnection getProxyConnection() {
		return ProxyConnection.newInstance( this );
	}

	@Override
	public ReactiveConnection getProxyConnection(String tenantId) {
		return ProxyConnection.newInstance( this, tenantId );
	}

}

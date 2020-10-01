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
 * <p>
 * This class may be extended by programs which wish to implement
 * custom connection management or multitenancy.
 *
 * @see SqlClientPoolConfiguration
 */
public abstract class BaseSqlClientPool implements ReactiveConnectionPool {

	protected abstract Pool getPool();

	protected abstract SqlStatementLogger getSqlStatementLogger();

	protected abstract boolean isUsePostgresStyleParameters();

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
		return new SqlClientConnection( connection, getPool(), getSqlStatementLogger(), isUsePostgresStyleParameters() );
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

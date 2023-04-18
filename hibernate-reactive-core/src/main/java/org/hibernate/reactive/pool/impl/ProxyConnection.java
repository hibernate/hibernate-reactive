/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.util.impl.CompletionStages;

import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.hibernate.reactive.common.InternalStateAssertions.assertUseOnEventLoop;

/**
 * A proxy {@link ReactiveConnection} that initializes the
 * underlying connection lazily.
 */
final class ProxyConnection implements ReactiveConnection {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveConnectionPool sqlClientPool;

	private ReactiveConnection connection;
	private boolean connected;
	private boolean closed;
	private final String tenantId;

	public ProxyConnection(ReactiveConnectionPool sqlClientPool) {
		this.sqlClientPool = sqlClientPool;
		this.tenantId = null;
	}

	public ProxyConnection(ReactiveConnectionPool sqlClientPool, String tenantId) {
		this.sqlClientPool = sqlClientPool;
		this.tenantId = tenantId;
	}

	@Override
	public DatabaseMetadata getDatabaseMetadata() {
		Objects.requireNonNull( connection, "Connection not available at the moment" );
		return connection.getDatabaseMetadata();
	}

	private <T> CompletionStage<T> withConnection(Function<ReactiveConnection, CompletionStage<T>> operation) {
		assertUseOnEventLoop();
		if ( closed ) {
			CompletableFuture<T> ret = new CompletableFuture<>();
			ret.completeExceptionally( LOG.sessionIsClosed() );
			return ret;
		}
		if ( !connected ) {
			connected = true; // we're not allowed to fetch two connections!
			CompletionStage<ReactiveConnection> connection =
					tenantId == null ? sqlClientPool.getConnection() : sqlClientPool.getConnection( tenantId );
			return connection.thenApply( newConnection -> this.connection = newConnection )
					.thenCompose( operation );
		}
		else {
			if ( connection == null ) {
				// we're already in the process of fetching a connection,
				// so this must be an illegal concurrent call
				CompletableFuture<T> ret = new CompletableFuture<>();
				ret.completeExceptionally( LOG.sessionIsConnectingToTheDatabase() );
				return ret;
			}
			return operation.apply( connection );
		}
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return withConnection( conn -> conn.execute( sql ) );
	}

	@Override
	public CompletionStage<Void> executeUnprepared(String sql) {
		return withConnection( conn -> conn.executeUnprepared( sql ) );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return withConnection( conn -> conn.executeOutsideTransaction( sql ) );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return withConnection( conn -> conn.update( sql ) );
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.update( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Void> update(
			String sql,
			Object[] paramValues,
			boolean allowBatching,
			Expectation expectation) {
		return withConnection( conn -> conn.update( sql, paramValues, allowBatching, expectation ) );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
		return withConnection( conn -> conn.update( sql, paramValues ) );
	}

	@Override
	public <T> CompletionStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName) {
		return withConnection( conn -> conn.insertAndSelectIdentifier( sql, paramValues, idClass, idColumnName ) );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return withConnection( conn -> conn.select( sql ) );
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.select( sql, paramValues ) );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.selectJdbc( sql, paramValues ) );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbcOutsideTransaction(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.selectJdbcOutsideTransaction( sql, paramValues ) );
	}

	@Override
	public <T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass) {
		return withConnection( conn -> conn.selectIdentifier( sql, paramValues, idClass ) );
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return withConnection( ReactiveConnection::beginTransaction );
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return withConnection( ReactiveConnection::commitTransaction );
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return withConnection( ReactiveConnection::rollbackTransaction );
	}

	@Override
	public ReactiveConnection withBatchSize(int batchSize) {
		connection = connection.withBatchSize( batchSize );
		return this;
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return withConnection( ReactiveConnection::executeBatch );
	}

	@Override
	public CompletionStage<Void> close() {
		CompletionStage<Void> stage = CompletionStages.voidFuture();
		if ( connection != null ) {
			stage = stage.thenCompose( v -> connection.close() );

		}
		return stage.thenAccept( v -> {
			connection = null;
			closed = true;
		} );
	}

}

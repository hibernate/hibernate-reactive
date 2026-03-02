/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

import io.vertx.sqlclient.spi.DatabaseMetadata;

/**
 * A wrapper for {@link ReactiveConnection} that is used when the connection
 * is externally managed (e.g., by Quarkus transaction management).
 * <p>
 * This wrapper delegates all operations to the underlying connection except
 * for {@link #close()}, which is a no-op since the external transaction
 * coordinator is responsible for closing the connection.
 * <p>
 * See {{@link #closeUnderlyingConnection()}} for actually closing the underlying
 * connection.
 */
public class ExternallyManagedConnection implements ReactiveConnection {

	private final ReactiveConnection delegate;
	private final ReactiveTransactionCoordinator coordinator;

	public ExternallyManagedConnection(ReactiveConnection delegate, ReactiveTransactionCoordinator coordinator) {
		this.delegate = delegate;
		this.coordinator = coordinator;
	}

	/**
	 * Wraps a connection for use in externally-managed transaction contexts.
	 * The wrapped connection's {@link #close()} method becomes a no-op.
	 *
	 * @param connection the connection to wrap
	 * @param coordinator the transaction coordinator that manages this connection
	 * @return a wrapped connection that won't close when {@link #close()} is called
	 */
	public static ExternallyManagedConnection wrap(ReactiveConnection connection, ReactiveTransactionCoordinator coordinator) {
		return new ExternallyManagedConnection( connection, coordinator );
	}

	@Override
	public boolean isTransactionInProgress() {
		return delegate.isTransactionInProgress();
	}

	@Override
	public ReactiveTransactionCoordinator getTransactionCoordinator() {
		return coordinator;
	}

	@Override
	public DatabaseMetadata getDatabaseMetadata() {
		return delegate.getDatabaseMetadata();
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return delegate.execute( sql );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return delegate.executeOutsideTransaction( sql );
	}

	@Override
	public CompletionStage<Void> executeUnprepared(String sql) {
		return delegate.executeUnprepared( sql );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return delegate.update( sql );
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return delegate.update( sql, paramValues );
	}

	@Override
	public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
		return delegate.update( sql, paramValues, allowBatching, expectation );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
		return delegate.update( sql, paramValues );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return delegate.select( sql );
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		return delegate.select( sql, paramValues );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return delegate.selectJdbc( sql, paramValues );
	}

	@Override
	@Deprecated
	public <T> CompletionStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName) {
		return delegate.insertAndSelectIdentifier( sql, paramValues, idClass, idColumnName );
	}

	@Override
	@Deprecated
	public CompletionStage<ResultSet> insertAndSelectIdentifierAsResultSet(String sql, Object[] paramValues, Class<?> idClass, String idColumnName) {
		return delegate.insertAndSelectIdentifierAsResultSet( sql, paramValues, idClass, idColumnName );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql) {
		return delegate.selectJdbc( sql );
	}

	@Override
	public CompletionStage<ResultSet> executeAndSelectGeneratedValues(String sql, Object[] paramValues, List<Class<?>> idClass, List<String> generatedColumnName) {
		return delegate.executeAndSelectGeneratedValues( sql, paramValues, idClass, generatedColumnName );
	}

	@Override
	public <T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass) {
		return delegate.selectIdentifier( sql, paramValues, idClass );
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return delegate.beginTransaction();
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return coordinator.beforeCompletion()
				.thenCompose( v -> delegate.commitTransaction() )
				.thenCompose( v -> coordinator.afterCompletion( ReactiveTransactionCoordinator.STATUS_COMMITTED ) );
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return delegate.rollbackTransaction()
				.thenCompose( v -> coordinator.afterCompletion( ReactiveTransactionCoordinator.STATUS_ROLLEDBACK ) );
	}

	@Override
	public ReactiveConnection withBatchSize(int batchSize) {
		return delegate.withBatchSize( batchSize );
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return delegate.executeBatch();
	}

	/**
	 * No-op. The external transaction coordinator is responsible for closing the connection.
	 *
	 * @return a completed future
	 */
	@Override
	public CompletionStage<Void> close() {
		// Don't close - the external coordinator manages the connection lifecycle
		return voidFuture();
	}

	/**
	 * Actually closes the underlying connection.
	 * Called by the external transaction manager when the transaction is complete.
	 *
	 * @return a completion stage that completes when the connection is closed
	 */
	public CompletionStage<Void> closeUnderlyingConnection() {
		return delegate.close();
	}
}

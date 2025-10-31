/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import io.vertx.core.Future;
import io.vertx.sqlclient.DatabaseException;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

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
	 * statements as they are executed
	 */
	protected abstract SqlStatementLogger getSqlStatementLogger();

	/**
	 * @return a Hibernate {@link SqlExceptionHelper} for converting
	 * exceptions
	 */
	protected abstract SqlExceptionHelper getSqlExceptionHelper();

	/**
	 * Get a {@link Pool} for the specified tenant.
	 * <p>
	 * This is an unimplemented operation which must be overridden by
	 * subclasses which support multitenancy.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @throws UnsupportedOperationException if multitenancy is not supported
	 * @see ReactiveConnectionPool#getConnection(String)
	 */
	protected Pool getTenantPool(String tenantId) {
		throw new UnsupportedOperationException( "multitenancy not supported by built-in SqlClientPool" );
	}

	@Override
	public ReactiveConnection getProxyConnection() {
		return new ProxyConnection( this::getConnection );
	}

	@Override
	public ReactiveConnection getProxyConnection(String tenantId) {
		return tenantId == null
				? new ProxyConnection( this::getConnection )
				: new ProxyConnection( () -> getConnection( tenantId ) );
	}

	@Override
	public ReactiveConnection getProxyConnection(SqlExceptionHelper sqlExceptionHelper) {
		return new ProxyConnection( () -> getConnection( sqlExceptionHelper ) );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection() {
		return getConnectionFromPool( getPool() );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection(SqlExceptionHelper sqlExceptionHelper) {
		return getConnectionFromPool( getPool(), sqlExceptionHelper );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection(String tenantId) {
		return getConnectionFromPool( getTenantPool( tenantId ) );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection(String tenantId, SqlExceptionHelper sqlExceptionHelper) {
		return getConnectionFromPool( getTenantPool( tenantId ), sqlExceptionHelper );
	}

	private CompletionStage<ReactiveConnection> getConnectionFromPool(Pool pool) {
		return completionStage( pool.getConnection().map( this::newConnection ), ReactiveConnection::close );
	}

	private CompletionStage<ReactiveConnection> getConnectionFromPool(Pool pool, SqlExceptionHelper sqlExceptionHelper) {
		return completionStage(
				pool.getConnection().map( sqlConnection -> newConnection( sqlConnection, sqlExceptionHelper ) ),
				ReactiveConnection::close
		);
	}

	/**
	 * This method is intended to be used only for queries returning
	 * a ResultSet that must be executed outside any "current"
	 * transaction (i.e. with autocommit=true).
	 * <p/>
	 * For example, it would be appropriate to use this method when
	 * performing queries on information_schema or system tables in
	 * order to obtain metadata information about catalogs, schemas,
	 * tables, etc.
	 *
	 * @param sql - the query to execute outside a transaction
	 * @param paramValues - a non-null array of parameter values
	 *
	 * @return the CompletionStage<ResultSet> from executing the query.
	 */
	public CompletionStage<ResultSet> selectJdbcOutsideTransaction(String sql, Object[] paramValues) {
		return preparedQueryOutsideTransaction( sql, Tuple.wrap( paramValues ) )
				.thenApply( ResultSetAdaptor::new );
	}

	private CompletionStage<RowSet<Row>> preparedQueryOutsideTransaction(String sql, Tuple parameters) {
		feedback( sql );
		return getPool().preparedQuery( sql ).execute( parameters ).toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) );
	}

	/**
	 * Similar to {@link org.hibernate.exception.internal.SQLExceptionTypeDelegate#convert(SQLException, String, String)}
	 */
	private <T> T convertException(T rows, String sql, Throwable sqlException) {
		if ( sqlException == null ) {
			return rows;
		}
		if ( sqlException instanceof DatabaseException de ) {
			sqlException = getSqlExceptionHelper()
					.convert(
							new SQLException( de.getMessage(), de.getSqlState(), de.getErrorCode() ),
							"error executing SQL statement",
							sql
					);
		}
		return rethrow( sqlException );
	}

	private void feedback(String sql) {
		Objects.requireNonNull( sql, "SQL query cannot be null" );
		// DDL already gets formatted by the client, so don't reformat it
		FormatStyle formatStyle = getSqlStatementLogger().isFormat() && !sql.contains( System.lineSeparator() )
				? FormatStyle.BASIC
				: FormatStyle.NONE;
		getSqlStatementLogger().logStatement( sql, formatStyle.getFormatter() );
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
			}
			else {
				completableFuture.completeExceptionally( ar.cause() );
			}
		} );
		return completableFuture;
	}

	private SqlClientConnection newConnection(SqlConnection connection) {
		return newConnection( connection, getSqlExceptionHelper() );
	}

	private SqlClientConnection newConnection(SqlConnection connection, SqlExceptionHelper sqlExceptionHelper) {
		return new SqlClientConnection( connection, getPool(), getSqlStatementLogger(), sqlExceptionHelper );
	}

	private static class ProxyConnection implements ReactiveConnection {
		private final Supplier<CompletionStage<ReactiveConnection>> connectionSupplier;
		private Integer batchSize;
		private ReactiveConnection connection;

		public ProxyConnection(Supplier<CompletionStage<ReactiveConnection>> connectionSupplier) {
			this.connectionSupplier = connectionSupplier;
		}

		/**
		 * @return the existing {@link ReactiveConnection}, or open a new one
		 */
		CompletionStage<ReactiveConnection> connection() {
			if ( connection == null ) {
				return connectionSupplier.get()
						.thenApply( conn -> {
							if ( batchSize != null ) {
								conn.withBatchSize( batchSize );
							}
							connection = conn;
							return connection;
						} );
			}
			return completedFuture( connection );
		}

		@Override
		public boolean isTransactionInProgress() {
			return connection != null && connection.isTransactionInProgress();
		}

		@Override
		public DatabaseMetadata getDatabaseMetadata() {
			Objects.requireNonNull( connection, "Database metadata not available until the connection is opened" );
			return connection.getDatabaseMetadata();
		}

		@Override
		public CompletionStage<Void> execute(String sql) {
			return connection().thenCompose( conn -> conn.execute( sql ) );
		}

		@Override
		public CompletionStage<Void> executeOutsideTransaction(String sql) {
			return connection().thenCompose( conn -> conn.executeOutsideTransaction( sql ) );
		}

		@Override
		public CompletionStage<Void> executeUnprepared(String sql) {
			return connection().thenCompose( conn -> conn.executeUnprepared( sql ) );
		}

		@Override
		public CompletionStage<Integer> update(String sql) {
			return connection().thenCompose( conn -> conn.update( sql ) );
		}

		@Override
		public CompletionStage<Integer> update(String sql, Object[] paramValues) {
			return connection().thenCompose( conn -> conn.update( sql, paramValues ) );
		}

		@Override
		public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
			return connection().thenCompose( conn -> conn.update( sql, paramValues, allowBatching, expectation ) );
		}

		@Override
		public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
			return connection().thenCompose( conn -> conn.update( sql, paramValues ) );
		}

		@Override
		public CompletionStage<Result> select(String sql) {
			return connection().thenCompose( conn -> conn.select( sql ) );
		}

		@Override
		public CompletionStage<Result> select(String sql, Object[] paramValues) {
			return connection().thenCompose( conn -> conn.select( sql ) );
		}

		@Override
		public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
			return connection().thenCompose( conn -> conn.selectJdbc( sql, paramValues ) );
		}

		@Override
		public <T> CompletionStage<T> insertAndSelectIdentifier(
				String sql,
				Object[] paramValues,
				Class<T> idClass,
				String idColumnName) {
			return connection().thenCompose( conn -> conn
					.insertAndSelectIdentifier( sql, paramValues, idClass, idColumnName ) );
		}

		@Override
		public CompletionStage<ResultSet> insertAndSelectIdentifierAsResultSet(
				String sql,
				Object[] paramValues,
				Class<?> idClass,
				String idColumnName) {
			return connection().thenCompose( conn -> conn
					.insertAndSelectIdentifierAsResultSet( sql, paramValues, idClass, idColumnName ) );
		}

		@Override
		public CompletionStage<ResultSet> selectJdbc(String sql) {
			return connection().thenCompose( conn -> conn.selectJdbc( sql ) );
		}

		@Override
		public CompletionStage<ResultSet> executeAndSelectGeneratedValues(
				String sql,
				Object[] paramValues,
				List<Class<?>> idClass,
				List<String> generatedColumnName) {
			return connection().thenCompose( conn -> conn
					.executeAndSelectGeneratedValues( sql, paramValues, idClass, generatedColumnName ) );
		}

		@Override
		public <T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass) {
			return connection().thenCompose( conn -> conn.selectIdentifier( sql, paramValues, idClass ) );
		}

		@Override
		public CompletionStage<Void> beginTransaction() {
			return connection().thenCompose( ReactiveConnection::beginTransaction );
		}

		@Override
		public CompletionStage<Void> commitTransaction() {
			return connection().thenCompose( ReactiveConnection::commitTransaction );
		}

		@Override
		public CompletionStage<Void> rollbackTransaction() {
			return connection().thenCompose( ReactiveConnection::rollbackTransaction );
		}

		@Override
		public ReactiveConnection withBatchSize(int batchSize) {
			if ( connection == null ) {
				this.batchSize = batchSize;
			}
			else {
				connection = connection.withBatchSize( batchSize );
			}
			return this;
		}

		@Override
		public CompletionStage<Void> executeBatch() {
			return connection().thenCompose( ReactiveConnection::executeBatch );
		}

		@Override
		public CompletionStage<Void> close() {
			return connection != null
					? connection.close().thenAccept( v -> connection = null )
					: voidFuture();
		}
	}
}

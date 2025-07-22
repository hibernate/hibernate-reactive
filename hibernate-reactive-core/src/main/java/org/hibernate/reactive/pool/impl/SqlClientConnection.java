/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.adaptor.impl.JdbcNull;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.util.impl.CompletionStages;

import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.DatabaseException;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive connection based on Vert.x's {@link SqlConnection}.
 */
public class SqlClientConnection implements ReactiveConnection {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	// See io.vertx.mysqlclient.MySQLClient#LAST_INSERTED_ID
	private final static PropertyKind<Long> MYSQL_LAST_INSERTED_ID = PropertyKind.create( "last-inserted-id", Long.class );
	// See io.vertx.oracleclient.OracleClient#GENERATED_KEYS
	private final static PropertyKind<Row> ORACLE_GENERATED_KEYS = PropertyKind.create( "generated-keys", Row.class );

	private final SqlStatementLogger sqlStatementLogger;
	private final SqlExceptionHelper sqlExceptionHelper;

	private final Pool pool;
	private final SqlConnection connection;
	// The context associated to the connection. We expect the connection to be executed in this context.
	private final ContextInternal connectionContext;

	// The close operation could be called multiple times if an error occurs,
	// if we execute it every time, we will have several useless messages in the log
	private boolean closed = false;

	SqlClientConnection(
			SqlConnection connection,
			Pool pool,
			SqlStatementLogger sqlStatementLogger,
			SqlExceptionHelper sqlExceptionHelper,
			ContextInternal connectionContext) {
		this.connectionContext = connectionContext;
		this.pool = pool;
		this.sqlStatementLogger = sqlStatementLogger;
		this.connection = connection;
		this.sqlExceptionHelper = sqlExceptionHelper;
		LOG.tracef( "Connection created for %1$s associated to context %2$s: ", connection, connectionContext );
	}

	@Override
	public DatabaseMetadata getDatabaseMetadata() {
		return client().databaseMetadata();
	}

	@Override
	public boolean isTransactionInProgress() {
		return connection.transaction() != null;
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		translateNulls( paramValues );
		return update( sql, Tuple.wrap( paramValues ) );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> batchParamValues) {
		final List<Tuple> tuples = new ArrayList<>( batchParamValues.size() );
		for ( Object[] paramValues : batchParamValues ) {
			translateNulls( paramValues );
			tuples.add( Tuple.wrap( paramValues ) );
		}
		return updateBatch( sql, tuples );
	}

	@Override
	public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
		return update( sql, paramValues )
				.thenAccept( rowCount -> expectation.verifyOutcome( rowCount, -1, sql ) );
	}

	@Override
	public <T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass) {
		translateNulls( paramValues );
		return preparedQuery( sql, Tuple.wrap( paramValues ) )
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) )
				.thenApply( rowSet -> {
					for ( Row row : rowSet ) {
						return row.get( idClass, 0 );
					}
					return null;
				} );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return preparedQuery( sql )
				.thenApply( RowSetResult::new );
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		translateNulls( paramValues );
		return preparedQuery( sql, Tuple.wrap( paramValues ) )
				.thenApply( RowSetResult::new );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		translateNulls( paramValues );
		return preparedQuery( sql, Tuple.wrap( paramValues ) )
				.thenApply( ResultSetAdaptor::new );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql) {
		return preparedQuery( sql )
				.thenApply( ResultSetAdaptor::new );
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return preparedQuery( sql )
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public CompletionStage<Void> executeUnprepared(String sql) {
		feedback( sql );
		return client().query( sql ).execute().toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) )
				.thenCompose( CompletionStages::voidFuture );
	}

	/**
	 * Similar to {@link org.hibernate.exception.internal.SQLExceptionTypeDelegate#convert(SQLException, String, String)}
	 */
	private <T> T convertException(T rows, String sql, Throwable sqlException) {
		if ( sqlException == null ) {
			return rows;
		}
		if ( sqlException instanceof DatabaseException de ) {
			sqlException = sqlExceptionHelper
					.convert( new SQLException( de.getMessage(), de.getSqlState(), de.getErrorCode() ), "error executing SQL statement", sql );
		}
		return rethrow( sqlException );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return preparedQueryOutsideTransaction( sql ).thenApply( ignore -> null );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return preparedQuery( sql ).thenApply( SqlResult::rowCount );
	}

	public CompletionStage<Integer> update(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters ).thenApply( SqlResult::rowCount );
	}

	public CompletionStage<int[]> updateBatch(String sql, List<Tuple> parametersBatch) {
		return preparedQueryBatch( sql, parametersBatch ).thenApply( result -> {

			final int[] updateCounts = new int[parametersBatch.size()];

			int i = 0;
			RowSet<Row> resultNext = result;
			if ( !parametersBatch.isEmpty() ) {
				final RowIterator<Row> iterator = resultNext.iterator();
				if ( iterator.hasNext() ) {
					while ( iterator.hasNext() ) {
						updateCounts[i++] = iterator.next().getInteger( 0 );
					}
					resultNext = null;
				}
				else {
					do {
						updateCounts[i++] = result.rowCount();
						resultNext = resultNext.next();
					} while ( resultNext != null && i < parametersBatch.size() );
				}
			}

			if ( resultNext != null || i != parametersBatch.size() ) {
				throw LOG.numberOfResultsGreaterThanBatchedParameters();
			}

			return updateCounts;
		} );
	}

	@Override
	public <T> CompletionStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName) {
		translateNulls( paramValues );
		return insertAndSelectIdentifier( sql, Tuple.wrap( paramValues ), idClass, idColumnName );
	}

	@Override
	public CompletionStage<ResultSet> insertAndSelectIdentifierAsResultSet(
			String sql,
			Object[] parameters,
			Class<?> idClass,
			String idColumnName) {
		return executeAndSelectGeneratedValues( sql, parameters, List.of( idClass ), List.of( idColumnName ) );
	}

	public <T> CompletionStage<T> insertAndSelectIdentifier(String sql, Tuple parameters, Class<T> idClass, String idColumnName) {
		// Oracle needs to know the name of the column id in advance, this shouldn't affect the other dbs
		JsonObject options = new JsonObject()
				.put( "autoGeneratedKeysIndexes", new JsonArray().add( idColumnName ) );

		return preparedQuery( sql, parameters, new PrepareOptions( options ) )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext()
							? iterator.next().get( idClass, 0 )
							: getLastInsertedId( rows, idClass, idColumnName );
				} );
	}

	@SuppressWarnings("unchecked")
	private static <T> T getLastInsertedId(RowSet<Row> rows, Class<T> idClass, String idColumnName) {
		final Long mySqlId = rows.property( MYSQL_LAST_INSERTED_ID );
		if ( mySqlId != null ) {
			if ( Long.class.equals( idClass ) ) {
				return (T) mySqlId;
			}
			if ( Integer.class.equals( idClass ) ) {
				return (T) ( Integer.valueOf( mySqlId.intValue() ) );
			}
			throw LOG.nativelyGeneratedValueMustBeLong();
		}
		final Row oracleKeys = rows.property( ORACLE_GENERATED_KEYS );
		if ( oracleKeys != null ) {
			return oracleKeys.get( idClass, idColumnName );
		}
		return null;
	}

	@Override
	public CompletionStage<ResultSet> executeAndSelectGeneratedValues(
			String sql,
			Object[] parameters,
			List<Class<?>> generatedValueClasses,
			List<String> generatedColumnName) {
		// Oracle needs to know the name of the column id in advance, this shouldn't affect the other dbs
		JsonArray autoGeneratedKeysIndexes = new JsonArray();
		generatedColumnName.forEach( autoGeneratedKeysIndexes::add );
		JsonObject options = new JsonObject()
				.put( "autoGeneratedKeysIndexes", autoGeneratedKeysIndexes );

		translateNulls( parameters );
		return preparedQuery( sql, Tuple.wrap( parameters ), new PrepareOptions( options ) )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext()
							? new ResultSetAdaptor( rows )
							: getLastInsertedGeneratedValuesAsResultSet( rows, generatedColumnName, generatedValueClasses );
				} );
	}

	private ResultSet getLastInsertedGeneratedValuesAsResultSet(
			RowSet<Row> rows,
			List<String> generatedColumnNames,
			List<Class<?>> generatedValueClasses) {
		final Long mySqlId = rows.property( MYSQL_LAST_INSERTED_ID );
		if ( mySqlId != null ) {
			// The MySQL Vert.x driver does not seem to support returning multiple generated values.
			final Class<?> idClass = generatedValueClasses.get( 0 );
			final String idColumnName = generatedColumnNames.get( 0 );
			if ( Long.class.equals( idClass ) ) {
				return new ResultSetAdaptor( rows, List.of( mySqlId ), idColumnName, Long.class );
			}
			if ( Integer.class.equals( idClass ) ) {
				return new ResultSetAdaptor( rows, List.of( mySqlId.intValue() ), idColumnName, Integer.class );
			}
			throw LOG.nativelyGeneratedValueMustBeLong();
		}
		final Row oracleKeys = rows.property( ORACLE_GENERATED_KEYS );
		if ( oracleKeys != null ) {
			return new ResultSetAdaptor( rows, ORACLE_GENERATED_KEYS, generatedColumnNames, generatedValueClasses );
		}
		return null;
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		feedback( sql );
		return client().preparedQuery( sql ).execute( parameters ).toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) );
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters, PrepareOptions options) {
		feedback( sql );
		return client().preparedQuery( sql, options ).execute( parameters ).toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) );
	}

	public CompletionStage<RowSet<Row>> preparedQueryBatch(String sql, List<Tuple> parameters) {
		feedback( sql );
		return client().preparedQuery( sql ).executeBatch( parameters ).toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) );
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		feedback( sql );
		return client().preparedQuery( sql ).execute().toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) );
	}

	public CompletionStage<RowSet<Row>> preparedQueryOutsideTransaction(String sql) {
		feedback( sql );
		return pool.preparedQuery( sql ).execute().toCompletionStage()
				.handle( (rows, throwable) -> convertException( rows, sql, throwable ) );
	}

	private void feedback(String sql) {
		// Disable assertions for now. See https://github.com/hibernate/hibernate-reactive/issues/2932
		// InternalStateAssertions.assertCurrentContextMatches( this, connectionContext );
		Objects.requireNonNull( sql, "SQL query cannot be null" );
		// DDL already gets formatted by the client, so don't reformat it
		FormatStyle formatStyle = sqlStatementLogger.isFormat() && !sql.contains( System.lineSeparator() )
				? FormatStyle.BASIC
				: FormatStyle.NONE;
		sqlStatementLogger.logStatement( sql, formatStyle.getFormatter() );
	}

	private SqlConnection client() {
		return connection;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		if ( isTransactionInProgress() ) {
			return failedFuture( LOG.liveTransactionDetectedOnBeginTransaction() );
		}
		return connection.begin()
				.onSuccess( tx -> LOG.tracef( "Transaction started: %s", tx ) )
				.onFailure( throwable -> LOG.errorf( "Failed to start a transaction: %s", throwable.getMessage() ) )
				.toCompletionStage()
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return connection.transaction()
				.commit()
				.onSuccess( v -> LOG.tracef( "Transaction committed: %s", connection.transaction() ) )
				.onFailure( throwable -> LOG.errorf( "Failed to commit transaction: %s", throwable.getMessage() ) )
				.toCompletionStage();
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		if ( isTransactionInProgress() ) {
			return connection.transaction()
					.rollback()
					.onFailure( throwable -> LOG.errorf( "Failed to rollback transaction: %s", throwable.getMessage() ) )
					.onSuccess( v -> LOG.tracef( "Transaction rolled back: %s", connection.transaction() ) )
					.toCompletionStage();
		}
		LOG.trace( "No transaction found to roll back" );
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> close() {
		// We can probably skip the validation if the connection is already closed...but, you never know
		return validateNoTransactionInProgressOnClose()
				.handle( CompletionStages::handle )
				.thenCompose( validationHandler -> supplyStage( () -> closed
						? voidFuture().thenAccept( v -> LOG.trace( "Connection already closed" ) )
						: connection.close().toCompletionStage() )
						.handle( CompletionStages::handle )
						.thenCompose( closeConnectionHandler -> {
							if ( closeConnectionHandler.hasFailed() ) {
								if ( validationHandler.hasFailed() ) {
									// Error closing the connection, include the validation error
									closeConnectionHandler.getThrowable()
											.addSuppressed( validationHandler.getThrowable() );
								}
								// Return a failed CompletionStage
								return closeConnectionHandler.getResultAsCompletionStage();
							}
							if ( !closed ) {
								closed = true;
								LOG.tracef( "Connection closed: %s", connection );
							}
							else {
								LOG.tracef( "Connection was already closed: %s", connection );
							}
							// Connection closed, return the result of the validation
							return validationHandler.getResultAsCompletionStage();
						} )
				);
	}

	/**
	 * If there's a transaction open, roll back it and return a failed CompletionStage.
	 * The validation error is related to closing the connection.
	 */
	private CompletionStage<Void> validateNoTransactionInProgressOnClose() {
		if ( isTransactionInProgress() ) {
			return supplyStage( this::rollbackTransaction )
					.handle( CompletionStages::handle )
					.thenCompose( rollbackHandler -> {
						final Throwable validationError = LOG.liveTransactionDetectedOnClose();
						if ( rollbackHandler.hasFailed() ) {
							// Include the error that happened during rollback
							validationError.addSuppressed( rollbackHandler.getThrowable() );
						}
						return failedFuture( validationError );
					} );
		}
		return voidFuture();
	}

	private static class RowSetResult implements Result {
		private final RowSet<Row> rowset;
		private final RowIterator<Row> it;

		public RowSetResult(RowSet<Row> rowset) {
			this.rowset = rowset;
			it = rowset.iterator();
		}

		@Override
		public int size() {
			return rowset.size();
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Object[] next() {
			Row row = it.next();
			Object[] result = new Object[row.size()];
			for ( int i = 0; i < result.length; i++ ) {
				result[i] = row.getValue( i );
			}
			return result;
		}
	}

	@Override
	public ReactiveConnection withBatchSize(int batchSize) {
		return batchSize <= 1
				? this
				: new BatchingConnection( this, batchSize );
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return voidFuture();
	}

	private static void translateNulls(Object[] paramValues) {
		for ( int i = 0; i < paramValues.length; i++ ) {
			Object arg = paramValues[i];
			if ( arg instanceof JdbcNull ) {
				paramValues[i] = ( (JdbcNull) arg ).toNullValue();
			}
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
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
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
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
	private Transaction transaction;

	SqlClientConnection(SqlConnection connection, Pool pool, SqlStatementLogger sqlStatementLogger, SqlExceptionHelper sqlExceptionHelper) {
		this.pool = pool;
		this.sqlStatementLogger = sqlStatementLogger;
		this.connection = connection;
		this.sqlExceptionHelper = sqlExceptionHelper;
		LOG.tracef( "Connection created: %s", connection );
	}

	@Override
	public DatabaseMetadata getDatabaseMetadata() {
		return client().databaseMetadata();
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
		if ( transaction != null ) {
			throw new IllegalStateException( "Can't begin a new transaction as an active transaction is already associated to this connection" );
		}
		return connection.begin()
				.onSuccess( tx -> LOG.tracef( "Transaction started: %s", tx ) )
				.onFailure( v -> LOG.errorf( "Failed to start a transaction: %s", transaction ) )
				.toCompletionStage()
				.thenAccept( this::setTransaction );
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return transaction.commit()
				.onSuccess( v -> LOG.tracef( "Transaction committed: %s", transaction ) )
				.onFailure( v -> LOG.errorf( "Failed to commit transaction: %s", transaction ) )
				.toCompletionStage()
				.whenComplete( this::clearTransaction );
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return transaction.rollback()
				.onFailure( v -> LOG.errorf( "Failed to rollback transaction: %s", transaction ) )
				.onSuccess( v -> LOG.tracef( "Transaction rolled back: %s", transaction ) )
				.toCompletionStage()
				.whenComplete( this::clearTransaction );
	}

	@Override
	public CompletionStage<Void> close() {
		if ( transaction != null ) {
			throw new IllegalStateException( "Connection being closed with a live transaction associated to it" );
		}
		return connection.close()
				.onSuccess( event -> LOG.tracef( "Connection closed: %s", connection ) )
				.onFailure( v -> LOG.errorf( "Failed to close a connection: %s", connection ) )
				.toCompletionStage();
	}

	private void setTransaction(Transaction tx) {
		transaction = tx;
	}

	private void clearTransaction(Void v, Throwable x) {
		LOG.tracef( "Clearing current transaction instance from connection: %s", transaction );
		transaction = null;
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

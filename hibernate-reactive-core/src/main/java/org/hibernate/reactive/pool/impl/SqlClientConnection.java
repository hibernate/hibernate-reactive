/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive connection based on Vert.x's {@link SqlConnection}.
 */
public class SqlClientConnection implements ReactiveConnection {

	private static PropertyKind<Long> mySqlLastInsertedId;

	private final SqlStatementLogger sqlStatementLogger;

	private final Pool pool;
	private final SqlConnection connection;
	private Transaction transaction;

	SqlClientConnection(SqlConnection connection, Pool pool,
						SqlStatementLogger sqlStatementLogger) {
		this.pool = pool;
		this.sqlStatementLogger = sqlStatementLogger;
		this.connection = connection;
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return update( sql, Tuple.wrap( paramValues ) );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> batchParamValues) {
		final List<Tuple> tuples = new ArrayList<>( batchParamValues.size() );
		for ( Object[] paramValues : batchParamValues) {
			tuples.add( Tuple.wrap( paramValues ) );
		}
		return updateBatch( sql, tuples );
	}

	@Override
	public CompletionStage<Void> update(String sql, Object[] paramValues,
										boolean allowBatching, Expectation expectation) {
		return update( sql, paramValues )
				.thenAccept( rowCount -> expectation.verifyOutcome( rowCount,-1, sql ) );
	}

	@Override
	public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
		return updateReturning( sql, Tuple.wrap( paramValues ) );
	}

	@Override
	public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
		return preparedQuery( sql, Tuple.wrap( paramValues ) )
				.thenApply( rowSet -> {
					for (Row row: rowSet) {
						return row.getLong(0);
					}
					return null;
				} );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return preparedQuery( sql ).thenApply(RowSetResult::new);
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		return preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(RowSetResult::new);
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(ResultSetAdaptor::new);
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return preparedQuery( sql ).thenApply( ignore -> null );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return preparedQueryOutsideTransaction( sql ).thenApply( ignore -> null );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return preparedQuery( sql ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Integer> update(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<int[]> updateBatch(String sql, List<Tuple> parametersBatch) {
		return preparedQueryBatch( sql, parametersBatch ).thenApply(result -> {

			final int[] updateCounts = new int[ parametersBatch.size() ];

			int i = 0;
			RowSet<Row> resultNext = result;
			if ( parametersBatch.size() > 0 ) {
				do {
					updateCounts[ i++ ] = resultNext.rowCount();
					resultNext = resultNext.next();
				} while ( resultNext != null && i < parametersBatch.size() );
			}

			if ( resultNext != null ) {
				throw new IllegalStateException( "Number of results is greater than number of batched parameters." );
			}

			if ( i != parametersBatch.size() ) {
				throw new IllegalStateException( "Number of results is not equal to number of batched parameters." );
			}

			return updateCounts;
		});
	}

	public CompletionStage<Long> updateReturning(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext() ?
							iterator.next().getLong(0) :
							rows.property(getMySqlLastInsertedId());
				} );
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		feedback(sql);
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( parameters, handler )
		);
	}

	public CompletionStage<RowSet<Row>> preparedQueryBatch(String sql, List<Tuple> parameters) {
		feedback(sql);
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( sql ).executeBatch( parameters, handler )
		);
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		feedback(sql);
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( handler )
		);
	}

	public CompletionStage<RowSet<Row>> preparedQueryOutsideTransaction(String sql) {
		feedback(sql);
		return Handlers.toCompletionStage(
				handler -> pool.preparedQuery( sql ).execute( handler )
		);
	}

	private void feedback(String sql) {
		Objects.requireNonNull(sql, "SQL query cannot be null");
		// DDL already gets formatted by the client, so don't reformat it
		FormatStyle formatStyle =
				sqlStatementLogger.isFormat() && !sql.contains( System.lineSeparator() )
						? FormatStyle.BASIC
						: FormatStyle.NONE;
		sqlStatementLogger.logStatement( sql, formatStyle.getFormatter() );
	}

	private SqlClient client() {
		return transaction != null ? transaction : connection;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		transaction = connection.begin();
		return voidFuture();
//		return execute("begin");
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return Handlers.toCompletionStage(
				handler -> transaction.commit(
						ar -> {
							transaction = null;
							handler.handle( ar );
						}
				)
		);
//		return execute("commit");
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return Handlers.toCompletionStage(
				handler -> transaction.rollback(
						ar -> {
							transaction = null;
							handler.handle( ar );
						}
				)
		);
//		return execute("rollback");
	}

	@Override
	public void close() {
		connection.close();
	}

	/**
	 * Loads MySQLClient.LAST_INSERTED_ID via reflection to avoid a hard
	 * dependency on the MySQL driver
	 */
	@SuppressWarnings("unchecked")
	private static PropertyKind<Long> getMySqlLastInsertedId() {
	    if (mySqlLastInsertedId == null) {
            try {
                Class<?> MySQLClient = Class.forName( "io.vertx.mysqlclient.MySQLClient" );
                mySqlLastInsertedId = (PropertyKind<Long>) MySQLClient.getField( "LAST_INSERTED_ID" ).get( null );
            } catch (ClassNotFoundException | NoSuchFieldException | IllegalArgumentException | IllegalAccessException | SecurityException e) {
                throw new RuntimeException( "Unable to obtain MySQLClient.LAST_INSERTED_ID field", e );
            }
        }
        return mySqlLastInsertedId;
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
			Object[] result = new Object[ row.size() ];
			for (int i=0; i<result.length; i++) {
				result[i] = row.getValue(i);
			}
			return result;
		}
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return voidFuture();
	}
}

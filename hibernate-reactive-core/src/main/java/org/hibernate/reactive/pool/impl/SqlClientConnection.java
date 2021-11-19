/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.adaptor.impl.JdbcNull;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.util.impl.CompletionStages;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive connection based on Vert.x's {@link SqlConnection}.
 */
public class SqlClientConnection implements ReactiveConnection {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final static PropertyKind<Long> mySqlLastInsertedId
			= PropertyKind.create("last-inserted-id", Long.class);

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
	public CompletionStage<Void> update(String sql, Object[] paramValues,
										boolean allowBatching, Expectation expectation) {
		return update( sql, paramValues )
				.thenAccept( rowCount -> expectation.verifyOutcome( rowCount,-1, sql ) );
	}

	@Override
	public CompletionStage<Long> insertAndSelectIdentifier(String sql, Object[] paramValues) {
		translateNulls( paramValues );
		return insertAndSelectIdentifier( sql, Tuple.wrap( paramValues ) );
	}

	@Override
	public CompletionStage<Long> selectIdentifier(String sql, Object[] paramValues) {
		translateNulls( paramValues );
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
		translateNulls( paramValues );
		return preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(RowSetResult::new);
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		translateNulls( paramValues );
		return preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(ResultSetAdaptor::new);
	}

	@Override
	public CompletionStage<ResultSet> selectJdbcOutsideTransaction(String sql, Object[] paramValues) {
		return preparedQueryOutsideTransaction( sql, Tuple.wrap( paramValues ) ).thenApply(ResultSetAdaptor::new);
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return preparedQuery( sql ).thenApply( ignore -> null );
	}

	@Override
	public CompletionStage<Void> executeUnprepared(String sql) {
		feedback( sql );
		return client().query( sql ).execute().toCompletionStage()
				.thenCompose( CompletionStages::voidFuture );
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
		});
	}

	public CompletionStage<Long> insertAndSelectIdentifier(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext() ?
							iterator.next().getLong(0) :
							getLastInsertedId(rows);
				} );
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		feedback( sql );
		return client().preparedQuery( sql ).execute( parameters ).toCompletionStage();
	}

	public CompletionStage<RowSet<Row>> preparedQueryBatch(String sql, List<Tuple> parameters) {
		feedback( sql );
		return client().preparedQuery( sql ).executeBatch( parameters ).toCompletionStage();
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		feedback( sql );
		return client().preparedQuery( sql ).execute().toCompletionStage();
	}

	public CompletionStage<RowSet<Row>> preparedQueryOutsideTransaction(String sql) {
		feedback( sql );
		return pool.preparedQuery( sql ).execute().toCompletionStage();
	}

	public CompletionStage<RowSet<Row>> preparedQueryOutsideTransaction(String sql, Tuple parameters) {
		feedback( sql );
		return pool.preparedQuery( sql ).execute( parameters ).toCompletionStage();
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
		return connection;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return connection.begin().toCompletionStage()
				.thenAccept( tx -> transaction = tx );
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return transaction.commit().toCompletionStage()
				.whenComplete( (v, x) -> transaction = null );
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return transaction.rollback().toCompletionStage()
				.whenComplete( (v, x) -> transaction = null );
	}

	@Override
	public CompletionStage<Void> close() {
		return connection.close().toCompletionStage();
	}

	private static Long getLastInsertedId(RowSet<Row> rows) {
		return rows.property(mySqlLastInsertedId);
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
	public ReactiveConnection withBatchSize(int batchSize) {
		return batchSize <= 1
				? this
				: new BatchingConnection(this, batchSize);
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return voidFuture();
	}

	private static void translateNulls(Object[] paramValues) {
		for (int i = 0; i < paramValues.length; i++) {
			Object arg = paramValues[i];
			if (arg instanceof JdbcNull) {
				paramValues[i] = ((JdbcNull) arg).toNullValue();
			}
		}
	}

}

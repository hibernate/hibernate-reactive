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
import org.hibernate.engine.jdbc.env.spi.AnsiSqlKeywords;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * A reactive connection based on Vert.x's {@link SqlConnection}.
 */
public class SqlClientConnection implements ReactiveConnection {

	private static final CoreMessageLogger log = CoreLogging.messageLogger("org.hibernate.SQL");

	private static PropertyKind<Long> mySqlLastInsertedId;

	private final boolean showSQL;
	private final boolean formatSQL;
	private final boolean highlightSQL;
	private final boolean usePostgresStyleParameters;

	private final Pool pool;
	private final SqlConnection connection;
	private Transaction transaction;

	SqlClientConnection(SqlConnection connection, Pool pool,
						boolean showSQL, boolean formatSQL, boolean highlightSQL,
						boolean usePostgresStyleParameters) {
		this.pool = pool;
		this.showSQL = showSQL;
		this.connection = connection;
		this.formatSQL = formatSQL;
		this.highlightSQL = highlightSQL;
		this.usePostgresStyleParameters = usePostgresStyleParameters;
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
		String processedSql = usePostgresStyleParameters ? Parameters.process( sql, parameters.size() ) : sql;
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( processedSql ).execute( parameters, handler )
		);
	}

	public CompletionStage<RowSet<Row>> preparedQueryBatch(String sql, List<Tuple> parameters) {
		feedback(sql);
		String processedSql = usePostgresStyleParameters ? Parameters.process( sql, parameters.size() ) : sql;
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( processedSql ).executeBatch( parameters, handler )
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

	private static final Pattern keywords =
			Pattern.compile(
					"\\b("
							+ String.join( "|", AnsiSqlKeywords.INSTANCE.sql2003() )
							+ "|"
							+ String.join( "|", "key", "sequence", "cascade", "increment" )
							+ ")\\b",
					Pattern.CASE_INSENSITIVE
			);
	private static final Pattern strings = Pattern.compile("'[^']*'");

	private void feedback(String sql) {
		Objects.requireNonNull(sql, "SQL query cannot be null");
		if ( showSQL || log.isDebugEnabled() ) {
			if ( formatSQL ) {
				//Note that DDL already gets formatted by the client
				if ( !sql.contains( System.lineSeparator() ) ) {
					sql = FormatStyle.BASIC.getFormatter().format(sql);
				}
			}
			if ( highlightSQL ) {
				//TODO: use FormatStyle.HIGHLIGHT when it gets added to Hibernate ORM core
				sql = keywords.matcher(sql).replaceAll("\u001b[34m$0\u001b[0m");
				sql = strings.matcher(sql).replaceAll("\u001b[36m$0\u001b[0m");
			}
		}

		log.debug( sql );

		if (showSQL) {
			String prefix = highlightSQL ? "\u001b[35m[Hibernate]\u001b[0m" : "Hibernate: ";
			System.out.println(prefix + sql);
		}
	}

	private SqlClient client() {
		return transaction != null ? transaction : connection;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		transaction = connection.begin();
		return CompletionStages.voidFuture();
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
		return CompletionStages.voidFuture();
	}
}

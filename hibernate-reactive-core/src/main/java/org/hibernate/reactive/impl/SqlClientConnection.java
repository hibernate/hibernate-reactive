package org.hibernate.reactive.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A reactive connection based on Vert.x's {@link SqlConnection}.
 */
public class SqlClientConnection implements ReactiveConnection {

	private final boolean showSQL;

	private final SqlConnection connection;
	private Transaction transaction;

	private SqlClientConnection(SqlConnection connection, boolean showSQL) {
		this.showSQL = showSQL;
		this.connection = connection;
	}

	public static CompletionStage<ReactiveConnection> create(Pool pool, boolean showSQL) {
		return toCompletionStage(
				handler -> pool.getConnection(
						ar -> handler.handle(
								ar.succeeded()
										? succeededFuture( new SqlClientConnection( ar.result(), showSQL ) )
										: failedFuture( ar.cause() )
						)
				)
		);
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return update( sql, Tuple.wrap( paramValues ) );
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
	public CompletionStage<Integer> update(String sql) {
		return preparedQuery( sql ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Integer> update(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Long> updateReturning(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext() ?
							iterator.next().getLong(0) :
							rows.property(MySQLClient.LAST_INSERTED_ID);
				} );
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		Objects.requireNonNull( sql, "SQL query cannot be null" );
		if (showSQL) {
			System.out.println(sql);
		}
		return toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( parameters, handler )
		);
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		Objects.requireNonNull( sql, "SQL query cannot be null" );
		if (showSQL) {
			System.out.println(sql);
		}
		return toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( handler )
		);
	}


	private SqlClient client() {
		return transaction != null ? transaction : connection;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		transaction = connection.begin();
		return CompletionStages.nullFuture();
//		return execute("begin");
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return toCompletionStage(
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
		return toCompletionStage(
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

	protected static <T> CompletionStage<T> toCompletionStage(
			Consumer<Handler<AsyncResult<T>>> completionConsumer) {
		CompletableFuture<T> cs = new CompletableFuture<>();
//		try {
			completionConsumer.accept( ar -> {
				if ( ar.succeeded() ) {
					cs.complete( ar.result() );
				}
				else {
					cs.completeExceptionally( ar.cause() );
				}
			} );
//		}
//		catch (Exception e) {
//			cs.completeExceptionally( e );
//		}
		return cs;
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
}

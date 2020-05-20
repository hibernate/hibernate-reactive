package org.hibernate.reactive.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;
import org.hibernate.reactive.service.ReactiveConnection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A reactive connection based on Vert.x's {@link Pool}.
 */
public class SqlClientConnection implements ReactiveConnection {

	protected final Pool pool;
	private final boolean showSQL;

	private SqlConnection connection;
	private Transaction transaction;

	public SqlClientConnection(Pool pool, boolean showSQL) {
		this.showSQL = showSQL;
		this.pool = pool;
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return preparedQuery( sql ).thenApply( ignore -> null );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return preparedQuery( sql ).thenApply(SqlResult::rowCount);
	}

	@Override
	public CompletionStage<Integer> update(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters ).thenApply(SqlResult::rowCount);
	}

	@Override
	public CompletionStage<Long> updateReturning(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext() ?
							iterator.next().getLong(0) :
							rows.property(MySQLClient.LAST_INSERTED_ID);
				} );
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		if (showSQL) {
			System.out.println(sql);
		}
		return toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( parameters, handler )
		);
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		if (showSQL) {
			System.out.println(sql);
		}
		return toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( handler )
		);
	}


	private SqlClient client() {
		return transaction != null ? transaction : pool;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return toCompletionStage(
				handler -> pool.getConnection(
						ar -> {
							if ( ar.succeeded() ) {
								connection = ar.result();
								transaction = connection.begin();
								handler.handle( succeededFuture() );
							}
							else {
								handler.handle( failedFuture( ar.cause() ) );
							}
						}
				)
		);
//		return execute("begin");
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return toCompletionStage(
				handler -> transaction.commit(
						ar -> {
							close();
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
							close();
							handler.handle( ar );
						}
				)
		);
//		return execute("rollback");
	}

	@Override
	public void close() {
		if ( connection != null ) {
			connection.close();
			connection = null;
			transaction = null;
		}
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

}

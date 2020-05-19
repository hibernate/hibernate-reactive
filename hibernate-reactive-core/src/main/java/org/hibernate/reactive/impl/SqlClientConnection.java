package org.hibernate.reactive.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;
import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.util.impl.CompletionStages;

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

	protected CompletionStage<? extends SqlClient> connection() {
		return CompletionStages.completedFuture( pool );
	}

	public SqlClientConnection(Pool pool, boolean showSQL) {
		this.showSQL = showSQL;
		this.pool = pool;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		throw new UnsupportedOperationException();
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
		return connection().thenCompose(
				client -> toCompletionStage(
						handler -> client.preparedQuery( sql ).execute(
								parameters,
								ar -> handler.handle( toFuture(ar) )
						)
				)
		);
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		if (showSQL) {
			System.out.println(sql);
		}
		return connection().thenCompose(
				client -> toCompletionStage(
						handler -> client.preparedQuery( sql ).execute(
								ar -> handler.handle( toFuture(ar) )
						)
				)
		);
	}

	protected static <T> CompletionStage<T> toCompletionStage(
			Consumer<Handler<AsyncResult<T>>> completionConsumer) {
		CompletableFuture<T> cs = new CompletableFuture<>();
		try {
			completionConsumer.accept( ar -> {
				if ( ar.succeeded() ) {
					cs.complete( ar.result() );
				}
				else {
					cs.completeExceptionally( ar.cause() );
				}
			} );
		}
		catch (Exception e) {
			// unsure we need this ?
			cs.completeExceptionally( e );
		}
		return cs;
	}

	protected static <T> Future<T> toFuture(AsyncResult<T> ar) {
		return ar.succeeded() ? succeededFuture( ar.result() ) : failedFuture( ar.cause() );
	}

	@Override
	public void close() {}
}

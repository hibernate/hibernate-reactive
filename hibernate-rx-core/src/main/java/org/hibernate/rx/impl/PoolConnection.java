package org.hibernate.rx.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A reactive connection based on Vert.x's {@link Pool}.
 */
public class PoolConnection implements RxConnection {

	private final Pool pool;
	private final boolean showSQL;

	public PoolConnection(Pool pool, boolean showSQL) {
		this.pool = pool;
		this.showSQL = showSQL;
	}

	@Override
	public CompletionStage<Void> inTransaction(
			Consumer<RxSession> consumer,
			RxSession delegate) {
		// Not used at the moment
		// Just an idea
//		return CompletableFuture.runAsync( () -> {
//			pool.getConnection( res -> {
//				if (res.succeeded()) {
//					// Transaction must use a connection
//					SqlConnection conn = res.result();
//
//					// Begin the transaction
//					Transaction tx = conn.begin();
//
//					// Commit the transaction
//					tx.commit(ar -> {
//						consumer.accept( delegate );
//					});
//				}
//			});
//		} );
		return null;
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
				.thenApply( rows -> rows.property(MySQLClient.LAST_INSERTED_ID) );
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		if (showSQL) {
			System.out.println(sql);
		}
		return toCompletionStage(
				handler -> pool.preparedQuery( sql ).execute(
						parameters,
						ar -> handler.handle( toFuture(ar) )
				)
		);
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		if (showSQL) {
			System.out.println(sql);
		}
		return toCompletionStage(
				handler -> pool.preparedQuery( sql ).execute(
						ar -> handler.handle( toFuture(ar) )
				)
		);
	}

	private static CompletionStage<RowSet<Row>> toCompletionStage(
			Consumer<Handler<AsyncResult<RowSet<Row>>>> completionConsumer) {
		CompletableFuture<RowSet<Row>> cs = new CompletableFuture<>();
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

	private static Future<RowSet<Row>> toFuture(AsyncResult<RowSet<Row>> ar) {
		return ar.succeeded() ? succeededFuture( ar.result() ) : failedFuture( ar.cause() );
	}

	@Override
	public void close() {
		// Nothing to do here, I think
	}
}

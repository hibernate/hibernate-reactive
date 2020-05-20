package org.hibernate.reactive.impl;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.util.concurrent.CompletionStage;

/**
 * A transaction-capable reactive connection based on Vert.x's
 * {@link SqlConnection}.
 */
public class TransactionalSqlClientConnection extends SqlClientConnection {

	private SqlConnection connection;
	private Transaction transaction;

	public TransactionalSqlClientConnection(Pool pool, boolean showSQL) {
		super(pool, showSQL);
	}

	protected CompletionStage<SqlConnection> connection() {
		if ( connection != null ) {
			return CompletionStages.completedFuture( connection );
		}
		else {
			return toCompletionStage(
					handler -> pool.getConnection(
							ar -> {
								if ( ar.succeeded() ) {
									connection = ar.result();
								}
								handler.handle( toFuture(ar) );
							}
					)
			);
		}
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return connection()
				.thenAccept( conn -> transaction = conn.begin() );
//		return execute("begin");
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return connection()
				.thenCompose(
						conn -> toCompletionStage(
								handler -> transaction.commit(
										ar-> handler.handle( toFuture(ar) )
								)
						)
				);
//		return execute("commit");
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return connection()
				.thenCompose(
						conn -> toCompletionStage(
								handler -> transaction.rollback(
										ar-> handler.handle( toFuture(ar) )
								)
						)
				);
//		return execute("rollback");
	}

	@Override
	public void close() {
		if ( connection != null ) {
			connection.close();
			connection = null;
		}
	}

}

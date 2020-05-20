package org.hibernate.reactive.impl;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.util.concurrent.CompletionStage;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

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

	protected SqlClient client() {
		return transaction != null
				? transaction
				: super.client();
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
		if ( connection != null ) {
			connection.close();
			connection = null;
			transaction = null;
		}
	}

}

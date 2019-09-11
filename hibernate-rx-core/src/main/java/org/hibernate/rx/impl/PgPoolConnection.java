package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.service.UnknownUnwrapTypeException;

import io.vertx.axle.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.axle.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.axle.sqlclient.SqlConnection;
import io.vertx.axle.sqlclient.Transaction;

/**
 * A reactive connection pool for PostgreSQL
 */
public class PgPoolConnection implements RxConnection {

	private final PoolOptions poolOptions;
    private final PgConnectOptions connectOptions;
	private final PgPool pool;

	public PgPoolConnection(PgConnectOptions connectOptions, PoolOptions poolOptions) {
		this.connectOptions = connectOptions;
		this.poolOptions = poolOptions;
		this.pool = PgPool.pool(Vertx.vertx(), connectOptions, poolOptions);
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
	public boolean isUnwrappableAs(Class unwrapType) {
		return PgPool.class.isAssignableFrom( unwrapType );
	}

	@Override
	public <T> T unwrap(Class<T> unwrapType) {
		if ( PgPool.class.isAssignableFrom( unwrapType ) ) {
			return (T) pool;
		}

		throw new UnknownUnwrapTypeException( unwrapType );
	}

}

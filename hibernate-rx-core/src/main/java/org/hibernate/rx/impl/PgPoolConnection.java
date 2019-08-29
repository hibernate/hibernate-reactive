package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.service.UnknownUnwrapTypeException;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.pgclient.PgTransaction;

/**
 * A reactive connection pool for PostgreSQL
 */
public class PgPoolConnection implements RxConnection {

	private final PgPoolOptions options;
	private final PgPool pool;

	public PgPoolConnection(PgPoolOptions options) {
		this.options = options;
		this.pool = PgClient.pool( options );
	}

	@Override
	public CompletionStage<Void> inTransaction(
			Consumer<RxSession> consumer,
			RxSession delegate) {
		return CompletableFuture.runAsync( () -> {
			pool.getConnection( res -> {
				if (res.succeeded()) {
					// Transaction must use a connection
					PgConnection conn = res.result();

					// Begin the transaction
					PgTransaction tx = conn.begin();

					// Commit the transaction
					tx.commit(ar -> {
						consumer.accept( delegate );
					});
				}
			});
		} );
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

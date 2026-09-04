/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.pool.ReactiveConnection;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for <a href="https://github.com/hibernate/hibernate-reactive/issues/4133">#4133</a>:
 * pool connection leaked when acquisition is cancelled before Vert.x delivers it.
 * <p>
 * When the {@link CompletionStage} returned by {@link SqlClientPool#getConnection()}
 * is cancelled while the Vert.x pool has not yet delivered the connection, the connection
 * must be closed when it eventually arrives, otherwise it leaks from the pool.
 */
public class SqlClientPoolCancellationTest {

	@Test
	void connectionDeliveredAfterCancellationMustBeClosed() throws InterruptedException {
		Promise<SqlConnection> pending = Promise.promise();
		AtomicBoolean closeCalled = new AtomicBoolean( false );
		CountDownLatch closeLatch = new CountDownLatch( 1 );

		SqlConnection sqlConnection = (SqlConnection) Proxy.newProxyInstance(
				SqlConnection.class.getClassLoader(),
				new Class<?>[]{ SqlConnection.class },
				(proxy, method, args) -> {
					if ( "close".equals( method.getName() ) && ( args == null || args.length == 0 ) ) {
						closeCalled.set( true );
						closeLatch.countDown();
						return Future.succeededFuture();
					}
					if ( "exceptionHandler".equals( method.getName() ) ) {
						return proxy;
					}
					return null;
				}
		);

		Pool pool = (Pool) Proxy.newProxyInstance(
				Pool.class.getClassLoader(),
				new Class<?>[]{ Pool.class },
				(proxy, method, args) -> {
					if ( "getConnection".equals( method.getName() ) ) {
						return pending.future();
					}
					return null;
				}
		);

		SqlClientPool clientPool = new SqlClientPool() {
			@Override
			protected Pool getPool() {
				return pool;
			}

			@Override
			protected SqlStatementLogger getSqlStatementLogger() {
				return new SqlStatementLogger();
			}

			@Override
			protected SqlExceptionHelper getSqlExceptionHelper() {
				return new SqlExceptionHelper( false );
			}

			@Override
			public CompletionStage<Void> getCloseFuture() {
				return null;
			}
		};

		CompletionStage<ReactiveConnection> stage = clientPool.getConnection();
		// Mutiny cancels the stage when the subscriber is cancelled (e.g. client disconnect)
		stage.toCompletableFuture().cancel( false );
		// Vert.x delivers the connection after cancellation
		pending.complete( sqlConnection );

		assertTrue(
				closeLatch.await( 2, TimeUnit.SECONDS ),
				"Connection delivered after cancellation should have been closed"
		);
	}
}

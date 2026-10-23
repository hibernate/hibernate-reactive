/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.JdbcSettings.ALLOW_METADATA_ON_BOOT;
import static org.hibernate.cfg.JdbcSettings.JAKARTA_JDBC_URL;

/**
 * The dialect is detected while the factory is created, usually from a thread that is not associated with
 * a Vert.x context. The connection used to detect it must still be used from the Vert.x context where it has
 * been acquired, even when the pool completes the connection request on another thread.
 *
 * @see <a href="https://github.com/hibernate/hibernate-reactive/issues/4385">#4385</a>
 */
public class DialectDetectionContextTest {

	private static final String THREAD_NAME = "not-a-vertx-thread";

	private static final ExecutorService NOT_A_VERTX_THREAD = Executors.newSingleThreadExecutor( runnable -> {
		Thread thread = new Thread( runnable, THREAD_NAME );
		thread.setDaemon( true );
		return thread;
	} );

	private static final AtomicInteger CONNECTIONS = new AtomicInteger();

	@AfterAll
	public static void shutdownExecutor() {
		NOT_A_VERTX_THREAD.shutdownNow();
	}

	@Test
	public void testConnectionCompletedOnAnotherThread() {
		final Properties properties = new Properties();
		properties.setProperty( ALLOW_METADATA_ON_BOOT, "true" );
		properties.setProperty( JAKARTA_JDBC_URL, DatabaseConfiguration.getJdbcUrl() );
		properties.setProperty( Settings.SQL_CLIENT_POOL, OtherThreadPool.class.getName() );

		final StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder();
		builder.applySettings( properties );
		try (StandardServiceRegistry registry = builder.enableAutoClose().build()) {
			final Dialect dialect = registry.getService( JdbcEnvironment.class ).getDialect();

			assertThat( dialect ).isNotNull();
			assertThat( CONNECTIONS.get() ).isPositive();
		}
	}

	/**
	 * Completes the connection requests on a thread that is not a Vert.x thread.
	 */
	public static class OtherThreadPool extends DefaultSqlClientPool {
		@Override
		public CompletionStage<ReactiveConnection> getConnection(SqlExceptionHelper sqlExceptionHelper) {
			return super.getConnection( sqlExceptionHelper )
					.thenApplyAsync( connection -> {
						assertThat( Thread.currentThread().getName() ).isEqualTo( THREAD_NAME );
						CONNECTIONS.incrementAndGet();
						return connection;
					}, NOT_A_VERTX_THREAD );
		}
	}
}

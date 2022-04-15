/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.AsyncResult;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.SqlConnection;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Normally the {@link MySQLContainer} checks for readiness by attempting to establish a JDBC connection
 * and seeing if it can run a test query. We need to override this default behavior so that we can
 * keep all JDBC drivers off of our test classpath.
 * This is not an issue for other containers (PostgreSQL and DB2) because they use container log scraping
 * by default.
 */
class VertxMySqlContainer extends MySQLContainer<VertxMySqlContainer> {

	public VertxMySqlContainer(DockerImageName dockerImageName) {
		super( dockerImageName );
	}

	@Override
	protected void waitUntilContainerStarted() {
		logger().info(
				"Waiting for database connection to become available at {} using query '{}'",
				getVertxUrl(),
				getTestQueryString()
		);

		// Repeatedly try and open a connection to the DB and execute a test query
		long start = System.currentTimeMillis();
		try {
			while ( System.currentTimeMillis() < start + ( 1000L * getStartupTimeoutSeconds() ) ) {
				try {
					if ( !isRunning() ) {
						Thread.sleep( 100L );
						continue; // Don't attempt to connect yet
					}

					AtomicReference<AsyncResult<?>> result = new AtomicReference<>();
					CountDownLatch countDown = new CountDownLatch( 1 );
					SqlConnection connection = createVertxConnection();
					connection.query( getTestQueryString() ).execute( ar -> {
						result.set( ar );
						countDown.countDown();
					} );
					await( countDown );
					if ( result.get().succeeded() ) {
						break;
					}
					else {
						throw result.get().cause();
					}
				}
				catch (Throwable e) {
					// ignore so that we can try again
					logger().debug( "Failure when trying test query", e );
					Thread.sleep( 100L );
				}
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ContainerLaunchException( "Container startup wait was interrupted", e );
		}

		logger().info( "Container is started (URL: {})", getVertxUrl() );
	}

	public String getVertxUrl() {
		return MySQLDatabase.buildJdbcUrlWithCredentials( getJdbcUrl().replace( "jdbc:", "" ) );
	}

	public SqlConnection createVertxConnection() {
		String url = getVertxUrl();
		CountDownLatch countDown = new CountDownLatch( 1 );
		AtomicReference<AsyncResult<SqlConnection>> result = new AtomicReference<>();
		MySQLPool.pool( url ).getConnection( ar -> {
			result.set( ar );
			countDown.countDown();
		} );
		await( countDown );
		SqlConnection con = result.get().result();
		if ( con != null ) {
			return con;
		}
		else {
			throw new ContainerLaunchException( "Failed to obtain a connection", result.get().cause() );
		}
	}

	@Override
	public Connection createConnection(String queryString) throws NoDriverFoundException {
		throw new UnsupportedOperationException();
	}

	private void await(CountDownLatch latch) {
		try {
			if ( !latch.await( getStartupTimeoutSeconds(), TimeUnit.SECONDS ) ) {
				throw new ContainerLaunchException( "Timeout: container didn't start within the expected time" );
			}
		}
		catch (InterruptedException e) {
			throw new ContainerLaunchException( "Container startup wait was interrupted", e );
		}
	}

}

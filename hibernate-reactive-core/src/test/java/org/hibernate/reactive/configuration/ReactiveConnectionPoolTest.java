/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.hibernate.engine.jdbc.internal.JdbcServicesImpl;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;
import org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.reactive.testing.TestingRegistryExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.core.VertxOptions;
import io.vertx.junit5.RunTestOnContext;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.AvailableSettings.PASS;
import static org.hibernate.cfg.AvailableSettings.URL;
import static org.hibernate.cfg.AvailableSettings.USER;
import static org.hibernate.reactive.BaseReactiveTest.test;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.getJdbcUrl;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = 3600, timeUnit = TimeUnit.SECONDS)
public class ReactiveConnectionPoolTest {

	@RegisterExtension
	public DBSelectionExtension dbSelection = DBSelectionExtension.runOnlyFor( POSTGRESQL );

	@RegisterExtension
	public TestingRegistryExtension registryExtension = new TestingRegistryExtension();

	@RegisterExtension
	static RunTestOnContext testOnContext = new RunTestOnContext( vertxOptions() );

	private static VertxOptions vertxOptions() {
		return new VertxOptions()
				.setBlockedThreadCheckInterval( 5 )
				.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
	}

	@BeforeEach
	protected void initServices() {
		registryExtension.initialize( testOnContext );
	}

	private ReactiveConnectionPool configureAndStartPool(Map<String, Object> config) {
		DefaultSqlClientPoolConfiguration poolConfig = new DefaultSqlClientPoolConfiguration();
		poolConfig.configure( config );
		registryExtension.addService( SqlClientPoolConfiguration.class, poolConfig );
		registryExtension.addService( JdbcServices.class, new JdbcServicesImpl() {
			@Override
			public SqlStatementLogger getSqlStatementLogger() {
				return new SqlStatementLogger();
			}
		} );

		DefaultSqlClientPool reactivePool = new DefaultSqlClientPool();
		reactivePool.injectServices( registryExtension.getServiceRegistry() );
		reactivePool.configure( config );
		reactivePool.start();
		return reactivePool;
	}

	@Test
	public void configureWithJdbcUrl(VertxTestContext context) {
		Map<String,Object> config = new HashMap<>();
		config.put( URL, getJdbcUrl() );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		test( context, verifyConnectivity( reactivePool ) );
	}

	@Test
	public void configureWithCredentials(VertxTestContext context) {
		// Set up URL with invalid credentials so we can ensure that
		// explicit USER and PASS settings take precedence over credentials in the URL
		String url = getJdbcUrl();
		url = url.replace( "user=" + DatabaseConfiguration.USERNAME, "user=bogus" );
		url = url.replace( "password=" + DatabaseConfiguration.PASSWORD, "password=bogus" );

		// Correct user/password are supplied explicitly in the config map and
		// should override the credentials in the URL
		Map<String,Object> config = new HashMap<>();
		config.put( URL, url );
		config.put( USER, DatabaseConfiguration.USERNAME );
		config.put( PASS, DatabaseConfiguration.PASSWORD );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		test( context, verifyConnectivity( reactivePool ) );
	}

	@Test
	public void configureWithWrongCredentials(VertxTestContext context) {
		Map<String,Object> config = new HashMap<>();
		config.put( URL, getJdbcUrl() );
		config.put( USER, "bogus" );
		config.put( PASS, "bogus" );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		test( context, assertThrown( PgException.class, verifyConnectivity( reactivePool ) )
				.thenAccept( e -> assertThat( e.getMessage() ).contains( "bogus" ) )
		);
	}

	private static CompletionStage<Void> verifyConnectivity(ReactiveConnectionPool reactivePool) {
		return reactivePool.getConnection().thenCompose(
				connection -> connection.select( "SELECT 1" )
						.thenAccept( result -> {
							assertNotNull( result );
							assertEquals( 1, result.size() );
							assertEquals( 1, result.next()[0] );
						} ) );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.internal.JdbcServicesImpl;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.TestingRegistryRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

@RunWith(VertxUnitRunner.class)
public class ReactiveConnectionPoolTest {

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TestingRegistryRule registryRule = new TestingRegistryRule();

	protected static void test(TestContext context, CompletionStage<?> cs) {
		// this will be added to TestContext in the next vert.x release
		Async async = context.async();
		cs.whenComplete( (res, err) -> {
			if ( err != null ) {
				context.fail( err );
			}
			else {
				async.complete();
			}
		} );
	}

	private ReactiveConnectionPool configureAndStartPool(Map<String, Object> config) {
		DefaultSqlClientPoolConfiguration poolConfig = new DefaultSqlClientPoolConfiguration();
		poolConfig.configure( config );
		registryRule.addService( SqlClientPoolConfiguration.class, poolConfig );
		registryRule.addService( JdbcServices.class, new JdbcServicesImpl() {
			@Override
			public SqlStatementLogger getSqlStatementLogger() {
				return new SqlStatementLogger();
			}
		} );
		DefaultSqlClientPool reactivePool = new DefaultSqlClientPool();
		reactivePool.injectServices( registryRule.getServiceRegistry() );
		reactivePool.configure( config );
		reactivePool.start();
		return reactivePool;
	}

	@Test
	public void configureWithJdbcUrl(TestContext context) {
		String url = DatabaseConfiguration.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( Settings.URL, url );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		verifyConnectivity( context, reactivePool );
	}

	@Test
	public void configureWithCredentials(TestContext context) {
		// Set up URL with invalid credentials so we can ensure that
		// explicit USER and PASS settings take precedence over credentials in the URL
		String url = DatabaseConfiguration.getJdbcUrl();
		url = url.replace( "user=" + DatabaseConfiguration.USERNAME, "user=bogus" );
		url = url.replace( "password=" + DatabaseConfiguration.PASSWORD, "password=bogus" );

		// Correct user/password are supplied explicitly in the config map and
		// should override the credentials in the URL
		Map<String,Object> config = new HashMap<>();
		config.put( Settings.URL, url );
		config.put( Settings.USER, DatabaseConfiguration.USERNAME );
		config.put( Settings.PASS, DatabaseConfiguration.PASSWORD );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		verifyConnectivity( context, reactivePool );
	}

	@Test
	public void configureWithWrongCredentials(TestContext context) {
		thrown.expect( CompletionException.class );
		thrown.expectMessage( "io.vertx.pgclient.PgException:" );
		thrown.expectMessage( "bogus" );

		String url = DatabaseConfiguration.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( Settings.URL, url );
		config.put( Settings.USER, "bogus" );
		config.put( Settings.PASS, "bogus" );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		verifyConnectivity( context, reactivePool );
	}

	private void verifyConnectivity(TestContext context, ReactiveConnectionPool reactivePool) {
		test( context, reactivePool.getConnection().thenCompose(
				connection -> connection.select( "SELECT 1")
						.thenApply( rows -> {
							context.assertNotNull( rows );
							context.assertEquals( 1, rows.size() );
							Object[] row = rows.next();
							context.assertEquals( 1, row[0] );
							return null;
						} ) ) );
	}

}

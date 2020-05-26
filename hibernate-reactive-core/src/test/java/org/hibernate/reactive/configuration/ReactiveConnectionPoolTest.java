/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.impl.SqlClientPool;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import org.hibernate.reactive.testing.TestingRegistryRule;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ReactiveConnectionPoolTest {

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
		SqlClientPool reactivePool = new SqlClientPool();
		reactivePool.injectServices( registryRule.getServiceRegistry() );
		reactivePool.configure( config );
		reactivePool.start();
		return reactivePool;
	}

	@Test
	public void configureWithJdbcUrl(TestContext context) {
		// This test doesn't need to rotate across all DBs and has PG-specific logic in it
		assumeTrue( DatabaseConfiguration.dbType() == DBType.POSTGRESQL );

		String url = DatabaseConfiguration.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		verifyConnectivity( context, reactivePool );
	}

	@Test
	public void configureWithCredentials(TestContext context) {
		// This test doesn't need to rotate across all DBs and has PG-specific logic in it
		assumeTrue( DatabaseConfiguration.dbType() == DBType.POSTGRESQL );

		// Set up URL with invalid credentials so we can ensure that
		// explicit USER and PASS settings take precedence over credentials in the URL
		String url = DatabaseConfiguration.getJdbcUrl();
		url = url.replace( "user=" + DatabaseConfiguration.USERNAME, "user=bogus" );
		url = url.replace( "password=" + DatabaseConfiguration.PASSWORD, "password=bogus" );

		// Correct user/password are supplied explicitly in the config map and
		// should override the credentials in the URL
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		config.put( AvailableSettings.USER, DatabaseConfiguration.USERNAME );
		config.put( AvailableSettings.PASS, DatabaseConfiguration.PASSWORD );
		ReactiveConnectionPool reactivePool = configureAndStartPool( config );
		verifyConnectivity( context, reactivePool );
	}

	@Test
	public void configureWithWrongCredentials(TestContext context) {
		// This test doesn't need to rotate across all DBs and has PG-specific logic in it
		assumeTrue( DatabaseConfiguration.dbType() == DBType.POSTGRESQL );

		thrown.expect( CompletionException.class );
		thrown.expectMessage( "io.vertx.pgclient.PgException:" );
		thrown.expectMessage( "\"bogus\"" );

		String url = DatabaseConfiguration.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		config.put( AvailableSettings.USER, "bogus" );
		config.put( AvailableSettings.PASS, "bogus" );
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

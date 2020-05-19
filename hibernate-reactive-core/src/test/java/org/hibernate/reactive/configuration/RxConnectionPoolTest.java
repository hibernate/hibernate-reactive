/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.reactive.cfg.AvailableRxSettings;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.PostgreSQLDatabase;
import org.hibernate.reactive.service.RxConnectionPoolProviderImpl;
import org.hibernate.reactive.service.initiator.RxConnectionPoolProvider;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;

@RunWith(VertxUnitRunner.class)
public class RxConnectionPoolTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	@Rule
	public ExpectedException thrown = ExpectedException.none();

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

	@Test
	public void configureWithPool(TestContext context) {
		String url = PostgreSQLDatabase.getJdbcUrl().substring( "jdbc:".length() );
		Pool pgPool = PgPool.pool( url );
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableRxSettings.VERTX_POOL, pgPool );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		verifyConnectivity( context, rxPool );
	}

	@Test
	public void configureWithIncorrectPoolType() {
		thrown.expect( ConfigurationException.class );
		thrown.expectMessage( "Setting " + AvailableRxSettings.VERTX_POOL + " must be configured with an instance of io.vertx.sqlclient.Pool but was configured with I'm a pool!" );

		Map<String,Object> config = new HashMap<>();
		config.put( AvailableRxSettings.VERTX_POOL, "I'm a pool!" );

		new RxConnectionPoolProviderImpl( config );
	}
	
	@Test
	public void configureWithJdbcUrl(TestContext context) {
		String url = PostgreSQLDatabase.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		verifyConnectivity( context, rxPool );
	}
	
	@Test
	public void configureWithCredentials(TestContext context) {
		// Set up URL with invalid credentials so we can ensure that
		// explicit USER and PASS settings take precedence over credentials in the URL
		String url = PostgreSQLDatabase.getJdbcUrl();
		url = url.replace( "user=" + DatabaseConfiguration.USERNAME, "user=bogus" );
		url = url.replace( "password=" + DatabaseConfiguration.PASSWORD, "password=bogus" );
		
		// Correct user/password are supplied explicitly in the config map and 
		// should override the credentials in the URL
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		config.put( AvailableSettings.USER, DatabaseConfiguration.USERNAME );
		config.put( AvailableSettings.PASS, DatabaseConfiguration.PASSWORD );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		verifyConnectivity( context, rxPool );
	}

	@Test
	public void configureWithWrongCredentials(TestContext context) {
		thrown.expect( CompletionException.class );
		thrown.expectMessage( "io.vertx.pgclient.PgException:" );
		thrown.expectMessage( "\"bogus\"" );

		String url = PostgreSQLDatabase.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		config.put( AvailableSettings.USER, "bogus" );
		config.put( AvailableSettings.PASS, "bogus" );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		verifyConnectivity( context, rxPool );
	}

	private void verifyConnectivity(TestContext context, RxConnectionPoolProvider rxPool) {
		test( context, rxPool.getConnection()
				.preparedQuery( "SELECT 1" )
				.thenAccept( rows -> {
					context.assertNotNull( rows );
					context.assertEquals( 1, rows.size() );
					Row row = rows.iterator().next();
					context.assertEquals( 1, row.getInteger( 0 ) );
				} ) );
	}

}

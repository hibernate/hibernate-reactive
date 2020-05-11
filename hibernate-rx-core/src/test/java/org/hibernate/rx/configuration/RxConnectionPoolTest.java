/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.rx.cfg.AvailableRxSettings;
import org.hibernate.rx.containers.DatabaseConfiguration;
import org.hibernate.rx.containers.PostgreSQLDatabase;
import org.hibernate.rx.service.RxConnectionPoolProviderImpl;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.junit.Test;

import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;

public class RxConnectionPoolTest {

	@Test
	public void configureWithPool() {
		String url = PostgreSQLDatabase.getJdbcUrl().substring( "jdbc:".length() );
		Pool pgPool = PgPool.pool( url );
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableRxSettings.VERTX_POOL, pgPool );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		verifyConnectivity( rxPool );
	}
	
	@Test
	public void configureWithIncorrectPoolType() {
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableRxSettings.VERTX_POOL, "I'm a pool!" );
		try {
			new RxConnectionPoolProviderImpl( config );
			fail("Expected a ConfigurationException but one was not thrown");
		} catch (ConfigurationException expected) {
		}
	}
	
	@Test
	public void configureWithJdbcUrl() {
		String url = PostgreSQLDatabase.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		verifyConnectivity( rxPool );
	}
	
	@Test
	public void configureWithCredentials() {
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
		verifyConnectivity( rxPool );
	}
	
	private void verifyConnectivity(RxConnectionPoolProvider rxPool) {
		Row row = null;
		try {
			row = rxPool.getConnection()
					.preparedQuery( "SELECT 1" )
					.thenApply( rows -> {
						assertEquals(1, rows.size());
						return rows.iterator().next();
					})
					.toCompletableFuture()
					.get( 10, TimeUnit.SECONDS );
		}
		catch (Exception e) {
			fail("Connectivity test failed: " + e.getMessage());
		}
		assertEquals(1, row.size());
		assertEquals(Integer.valueOf( 1 ), row.getInteger( 0 ));
	}

}

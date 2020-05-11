/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.configuration;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.rx.cfg.AvailableRxSettings;
import org.hibernate.rx.containers.PostgreSQLDatabase;
import org.hibernate.rx.service.RxConnectionPoolProviderImpl;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.junit.Test;

import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;

public class RxConnectionPoolTest {

	@Test
	public void configureWithPool() {
		String url = PostgreSQLDatabase.getJdbcUrl().substring( "jdbc:".length() );
		Pool pgPool = PgPool.pool( url );
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableRxSettings.VERTX_POOL, pgPool );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		rxPool.getConnection().close();
	}
	
	@Test
	public void configureWithJdbcUrl() {
		String url = PostgreSQLDatabase.getJdbcUrl();
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		rxPool.getConnection().close();
	}
	
	@Test
	public void configureWithCredentials() {
		// Set up URL with invalid credentials so we can ensure settings are used
		String url = PostgreSQLDatabase.getJdbcUrl();
		url.replace( "user=" + PostgreSQLDatabase.USERNAME, "user=bogus" );
		url.replace( "password=" + PostgreSQLDatabase.PASSWORD, "password=bogus" );
		Map<String,Object> config = new HashMap<>();
		config.put( AvailableSettings.URL, url );
		config.put( AvailableSettings.USER, PostgreSQLDatabase.USERNAME );
		config.put( AvailableSettings.PASS, PostgreSQLDatabase.PASSWORD );
		RxConnectionPoolProvider rxPool = new RxConnectionPoolProviderImpl( config );
		rxPool.getConnection().close();
	}

}

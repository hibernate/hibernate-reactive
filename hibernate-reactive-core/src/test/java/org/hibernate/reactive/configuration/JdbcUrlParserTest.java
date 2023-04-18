/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hibernate.HibernateException;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;

import org.junit.Assert;
import org.junit.Test;

import io.vertx.sqlclient.SqlConnectOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hibernate.cfg.AvailableSettings.URL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.createJdbcUrl;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Test the correct creation of the {@link SqlConnectOptions}
 * given a JDBC connection URL.
 * <p>
 * This test doesn't require docker.
 */
public class JdbcUrlParserTest {

	private static final String DEFAULT_DB = "hreactDB";

	@Test
	public void test() throws Exception {
		CompletableFuture<Void> c1 = CompletableFuture
				.runAsync( () -> {
					try {
						System.out.println( "Start CF 1" );
						Thread.sleep( 2000 );
						System.out.println( 1 );
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				});

		CompletableFuture<Void> c2 = CompletableFuture
				.runAsync(() -> {
					try {
						System.out.println( "Start CF 2" );
						Thread.sleep( 100 );
						System.out.println( 2 );
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				});

		long start = System.currentTimeMillis();
		try {
			c1.get( 50, TimeUnit.MILLISECONDS );
		}
		catch (TimeoutException e) {
			System.out.println( "CF interrupted after " + ( System.currentTimeMillis() - start ) + "ms" );
			c1.cancel( true );
		}
		c2.get();
	}

	@Test
	public void exceptionWhenNull() {
		assertThatExceptionOfType( HibernateException.class )
				.isThrownBy( () -> DefaultSqlClientPool.parse( null ) )
				.withMessageStartingWith( "HR000079: The configuration property '" + URL + "' was not provided" );
	}

	@Test
	public void missingUser() {
		assertThatExceptionOfType( HibernateException.class )
				.isThrownBy( () -> {
					String url = createJdbcUrl( "localhost", dbType().getDefaultPort(), DEFAULT_DB, Map.of() );
					URI uri = DefaultSqlClientPool.parse( url );
					new DefaultSqlClientPoolConfiguration().connectOptions( uri );
				} )
				.withMessageStartingWith( "HR000019: database username not specified" );
	}

	@Test
	public void testOptionsWithExtraProperties() {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );
		params.put( "param1", "value1" );
		params.put( "param2", "value2" );
		params.put( "param3", "===value3===" );

		String url = createJdbcUrl( "localhost", dbType().getDefaultPort(), DEFAULT_DB, params );
		assertOptions( url, DEFAULT_DB, params );
	}

	@Test
	public void testOptionsWithoutExtraProperties() {
		// Without a user we would have an exception
		Map<String, String> params = new HashMap<>();
		params.put( "user", "PerryThePlatypus" );

		String url = createJdbcUrl( "localhost", dbType().getDefaultPort(), DEFAULT_DB, params );
		assertOptions( url, DEFAULT_DB, params );
	}

	@Test
	public void testOptionsWithPasswordAndProperties() {
		Map<String, String> params = new HashMap<>();
		params.put( "password", "helloPwd" );
		params.put( "user", "username" );
		params.put( "param2", "Value2" );

		String url = createJdbcUrl( "localhost", dbType().getDefaultPort(), DEFAULT_DB, params );
		assertOptions( url, DEFAULT_DB, params );
	}

	@Test
	public void testUrlWithoutPort() {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "PerryThePlatypus" );
		params.put( "password", "XxXxX" );
		params.put( "param2", "Value2" );

		String url = createJdbcUrl( "localhost", -1, DEFAULT_DB, params );
		assertOptions( url, DEFAULT_DB, params );
	}

	@Test
	public void testDatabaseAsProperty() {
		Map<String, String> params = new HashMap<>();
		params.put( "database", "helloDatabase" );
		params.put( "user", "PerryThePlatypus" );
		params.put( "password", "XxXxX" );
		params.put( "param2", "Value2" );

		String url = createJdbcUrl( "localhost", dbType().getDefaultPort(), null, params );
		assertOptions( url, "helloDatabase", params );
	}

	// URI regex does not include the '_' underscore character, so URI parsing will set the `host` and `userInfo`
	// to NULL.  This test verifies that the processing captures the actual host value and includes it in the
	// connect options. Example:  postgresql://local_host:5432/my_schema
	@Test
	public void testInvalidHostSucceeds() {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );

		String url = createJdbcUrl( "local_host", dbType().getDefaultPort(), "my_db", params );
		assertOptions( url, "my_db", "local_host", params );
	}

	@Test
	public void testInvalidHostWithoutPort() {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );

		// Port -1, so it won't be added to the url
		String url = createJdbcUrl( "local_host", -1, "my_db", params );
		assertOptions( url, "my_db", "local_host", params );
	}

	@Test
	public void testDefaultPortIsSet()  {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );

		// Port -1, so it won't be added to the url
		String url = createJdbcUrl( "localhost", -1, "my_db", params );
		assertOptions( url, "my_db", "localhost", params );
	}

	@Test
	public void testCustomPortIsSetWithInvalidUri() {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );

		String url = createJdbcUrl( "local_host", 19191, "my_db", params );
		assertOptions( url, "my_db", "local_host", 19191, params );
	}

	@Test
	public void testCustomPortIsSetWithValidUri()  {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );

		String url = createJdbcUrl( "myPersonalHost", 19191, "my_db", params );
		assertOptions( url, "my_db", "myPersonalHost", 19191, params );
	}

	@Test
	public void testUnrecognizedSchemeException()  {
		Assert.assertThrows( IllegalArgumentException.class, () -> {
			URI uri = new URI( "bogusScheme://localhost/database" );
			new DefaultSqlClientPoolConfiguration().connectOptions( uri );
		} );
	}


	private void assertOptions(String url, String expectedDbName, Map<String, String> parameters) {
		assertOptions( url, expectedDbName, "localhost", parameters );
	}

	private void assertOptions(String url, String expectedDbName, String expectedHost, Map<String, String> parameters) {
		assertOptions( url, expectedDbName, expectedHost, dbType().getDefaultPort(), parameters );
	}

	private void assertOptions(String url, String expectedDbName, String expectedHost, int expectedPort, Map<String, String> parameters) {
		URI uri = DefaultSqlClientPool.parse( url );
		SqlConnectOptions options = new DefaultSqlClientPoolConfiguration().connectOptions( uri );

		// These keys won't be mapped as properties
		String username = parameters.remove( "user" );
		String password = parameters.remove( "password" );
		parameters.remove( "database" );

		assertThat( options ).as( "URL: " + url ).isNotNull();
		assertThat( options.getUser() ).as( "URL: " + url ).isEqualTo( username );
		assertThat( options.getPassword() ).as( "URL: " + url ).isEqualTo( password );
		assertThat( options.getDatabase() ).as( "URL: " + url ).isEqualTo( expectedDbName );
		assertThat( options.getHost() ).as( "URL: " + url ).isEqualTo( expectedHost );
		assertThat( options.getPort() ).as( "URL: " + url ).isEqualTo( expectedPort );

		// Check extra properties
		assertThat( options.getProperties() ).as( "URL: " + url ).containsExactlyInAnyOrderEntriesOf( parameters );
	}
}

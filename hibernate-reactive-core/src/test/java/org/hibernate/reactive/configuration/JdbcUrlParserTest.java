/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.HibernateError;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;

import org.junit.Test;

import io.vertx.sqlclient.SqlConnectOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.createJdbcUrl;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.Assert.assertThrows;

/**
 * Test the correct creation of the {@link SqlConnectOptions}
 * given a JDBC connection URL.
 * <p>
 * This test doesn't require docker.
 */
public class JdbcUrlParserTest {

	@Test
	public void returnsNullForNull() {
		final HibernateError error = assertThrows( HibernateError.class, () -> {
			URI uri = DefaultSqlClientPool.parse( null );
			assertThat( uri ).isNull();
		} );
		assertThat( error.getMessage() ).contains( "was not provided" );
	}

	@Test
	public void missingUser() {
		final HibernateError error = assertThrows( HibernateError.class, () -> {
			URI uri = DefaultSqlClientPool.parse( defaultUrl() );
			new DefaultSqlClientPoolConfiguration().connectOptions( uri );
		} );
		assertThat( error.getMessage() ).contains( "database username not specified" );
	}

	@Test
	public void testOptionsWithExtraProperties() {
		Map<String, String> params = new HashMap<>();
		params.put( "user", "hello" );
		params.put( "param1", "value1" );
		params.put( "param2", "value2" );
		params.put( "param3", "value3" );

		assertOptions( createUrl( params ), defaultDb(), params );
	}

	@Test
	public void testOptionsWithoutExtraProperties() {
		// Without a user we would have an exception
		Map<String, String> params = new HashMap<>();
		params.put( "user", "PerryThePlatypus" );

		assertOptions( createUrl( params ), defaultDb(), params );
	}

	@Test
	public void testOptionsWithPasswordAndProperties() {
		Map<String, String> params = new HashMap<>();
		params.put( "password", "helloPwd" );
		params.put( "user", "username" );
		params.put( "param2", "Value2" );

		assertOptions( createUrl( params ), defaultDb(), params );
	}

	@Test
	public void testDatabaseAsProperty() {
		Map<String, String> params = new HashMap<>();
		params.put( "database", "helloDatabase" );
		params.put( "user", "PerryThePlatypus" );
		params.put( "password", "XxXxX" );
		params.put( "param2", "Value2" );

		assertOptions( createUrl( params ), "helloDatabase", params );
	}

	/**
	 * Create the default {@link SqlConnectOptions} with the given extra properties
	 * and assert that's correct.
	 *
	 * @return the created options in case a test needs custom extra assertions
	 */
	private SqlConnectOptions assertOptions(String url, String expectedDbName, Map<String, String> parameters) {
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
		assertThat( options.getHost() ).as( "URL: " + url ).isEqualTo( "localhost" );
		assertThat( options.getPort() ).as( "URL: " + url ).isEqualTo( dbType().getDefaultPort() );

		// Check extra properties
		assertThat( options.getProperties() ).as( "URL: " + url ).containsExactlyInAnyOrderEntriesOf( parameters );
		return options;
	}

	private String defaultUrl() {
		return createUrl( new HashMap<>() );
	}

	/**
	 * Create the JDBC Url with the additional extra properties (if any)
	 */
	private String createUrl(Map<String, String> properties) {
		return createJdbcUrl( "localhost", dbType().getDefaultPort(), defaultDb(), properties );
	}

	private String defaultDb() {
		return dbType() == SQLSERVER ? "" : "hreactDB";
	}
}

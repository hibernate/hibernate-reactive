/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import org.hibernate.HibernateError;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Map;

import io.vertx.db2client.DB2ConnectOptions;
import io.vertx.mssqlclient.MSSQLConnectOptions;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.SqlConnectOptions;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcUrlParserTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private SqlConnectOptions getUriPoolConnectOptions(String url) {
		URI uri = DefaultSqlClientPool.parse( url);
		DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
		final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
		return connectOptions;
	}

	private SqlConnectOptions verifyURLWithPort(int expectedPort, String url, Map<String, String> expectedProperties) {
		assertThat( url.startsWith( "jdbc" ) ).isTrue();

		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getDatabase() ).isEqualTo( "hreact" );
		assertThat( connectOptions.getHost() ).isEqualTo( "localhost" );
		assertThat( connectOptions.getPort() ).isEqualTo( expectedPort );
		assertThat( connectOptions.getUser() ).isEqualTo( "testuser" );
		assertThat( connectOptions.getPassword() ).isEqualTo( "testpassword" );

		for( Object key : expectedProperties.keySet() ) {
			assertThat(connectOptions.getProperties().keySet().contains( key )).isTrue();
			assertThat(connectOptions.getProperties().get(key)).isEqualTo( expectedProperties.get(key) );
		}

		return connectOptions;
	}

	private SqlConnectOptions verifyURLWithoutPort(int expectedPort, String url, Map<String, String> expectedProperties) {
		assertThat( url.startsWith( "jdbc" ) ).isTrue();

		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getDatabase() ).isEqualTo( "hreact" );
		assertThat( connectOptions.getHost() ).isEqualTo( "localhost" );
		assertThat( connectOptions.getPort() ).isEqualTo( expectedPort );
		assertThat( connectOptions.getUser() ).isEqualTo( "testuser" );
		assertThat( connectOptions.getPassword() ).isEqualTo( "testpassword" );

		for( Object key : expectedProperties.keySet() ) {
			assertThat(connectOptions.getProperties().keySet().contains( key )).isTrue();
			assertThat(connectOptions.getProperties().get(key)).isEqualTo( expectedProperties.get(key) );
		}

		return connectOptions;
	}

	@Test
	public void returnsNullForNull() {
		thrown.expect( HibernateError.class );
		URI uri = DefaultSqlClientPool.parse( null );
		assertThat( uri ).isNull();
	}

	@Test(expected = IllegalArgumentException.class)
	public void invalidParameter() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact?badvalue");
		DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
		final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
	}

	@Test
	public void parameters() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact?user=hello");
		DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
		final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
		assertThat(connectOptions).isNotNull();
	}

	@Test
	public void uriCreation() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).isNotNull();
	}

	@Test
	public void parsePort() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).hasPort(5432);
	}

	@Test
	public void parseHost() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).hasHost("localhost");
	}

	@Test
	public void parseScheme() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).hasScheme("postgresql");
	}

	// Example format from https://vertx.io/docs/vertx-pg-client/java/#_configuration
	@Test
	public void testPG() {
		String url = "jdbc:postgresql://testuser:testpassword@localhost:11111/hreact";
		verifyURLWithPort( 11111, url, PgConnectOptions.DEFAULT_PROPERTIES );
	}

	@Test
	public void testPG_2() {
		String url = "jdbc:postgresql://localhost:11111/hreact?loggerLevel=OFF&user=testuser&password=testpassword";
		verifyURLWithPort( 11111, url, PgConnectOptions.DEFAULT_PROPERTIES );
	}

	@Test
	public void testPGNoPort() {
		// DefaultSqlClientPoolConfiguration should check for port and apply a default if none found on URL
		String url = "jdbc:postgresql://testuser:testpassword@localhost/hreact";
		verifyURLWithPort( 5432, url, PgConnectOptions.DEFAULT_PROPERTIES );
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPGInvalidUrl() {
		String url = "jdbc:postgresql://testuser:testpassword@localhost/hreact%%%%xxxx=yyyyy";
		verifyURLWithPort( 5432, url, PgConnectOptions.DEFAULT_PROPERTIES );
	}

	@Test
	public void testPGUnsupportedProperties() {
		String url = "jdbc:postgresql://testuser:testpassword@localhost/hreact?loggerLevel=OFF";
		SqlConnectOptions connectOptions = verifyURLWithPort( 5432, url, PgConnectOptions.DEFAULT_PROPERTIES );
		assertThat(connectOptions.getProperties().keySet().contains( "loggerLevel" )).isFalse();
	}

	@Test
	public void testPGPassword() {
		String url = "jdbc:postgresql://testuser:~!HReact!~@localhost/hreact";
		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getPort() ).isEqualTo( 5432 );
		assertThat( connectOptions.getPassword() ).isEqualTo( "~!HReact!~" );
	}

	@Test
	public void testCockroachDBPassword() {
		String url = "jdbc:cockroachdb://testuser:~!HReact!~@localhost/hreact";
		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getPort() ).isEqualTo( 5432 );
		assertThat( connectOptions.getPassword() ).isEqualTo( "~!HReact!~" );
	}

	// Example format https://vertx.io/docs/vertx-mysql-client/java/#_configuration
	@Test
	public void testMYSQL() {
		String url = "jdbc:mysql://localhost:22222/hreact?user=testuser&password=testpassword";
		verifyURLWithPort( 22222, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
	}

	@Test
	public void testMYSQLUnsupportedProperties() {
		String url = "jdbc:mysql://localhost:22222/hreact?user=testuser&password=testpassword&prop_1=value_1";
		SqlConnectOptions connectOptions = verifyURLWithPort( 22222, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
		assertThat(connectOptions.getProperties().keySet().contains( "prop_1" )).isFalse();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMYSQLInvalidUrl() {
		String url = "jdbc:mysql://localhost:22222/hreact?user=testuser&password=testpassword%%%%xxxx=yyyyy";
		SqlConnectOptions connectOptions = verifyURLWithPort( 22222, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
		assertThat(connectOptions.getProperties().keySet().contains( "xxxx" )).isFalse();
	}

	@Test
	public void testMARIADB() {
		String url = "jdbc:mariadb://localhost:3306/hreact?user=testuser&password=testpassword";
		verifyURLWithPort( 3306, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
	}

	@Test
	public void testMARIADBNonDefaultPort() {
		String url = "jdbc:mariadb://localhost:22222/hreact?user=testuser&password=testpassword";
		verifyURLWithPort( 22222, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
	}

	@Test
	public void testMARIADBUnsupportedProperties() {
		String url = "jdbc:mariadb://localhost:3306/hreact?user=testuser&password=testpassword&prop_1=value_1";
		SqlConnectOptions connectOptions = verifyURLWithPort( 3306, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
		assertThat(connectOptions.getProperties().keySet().contains( "prop_1" )).isFalse();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMARIADBInvalidUrl() {
		String url = "jdbc:mariadb://localhost:22222/hreact?user=testuser&password=testpassword%%%%xxxx=yyyyy";
		SqlConnectOptions connectOptions = verifyURLWithPort( 22222, url, MySQLConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
		assertThat(connectOptions.getProperties().keySet().contains( "xxxx" )).isFalse();
	}

	@Test
	public void testMARIADBPassword() {
		String url = "jdbc:mariadb://testuser:~!HReact!~@localhost:3306/hreact";
		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getPort() ).isEqualTo( 3306 );
		assertThat( connectOptions.getPassword() ).isEqualTo( "~!HReact!~" );
	}

	// Example format https://vertx.io/docs/vertx-mssql-client/java/#_configuration
	@Test
	public void testMSSQL() {
		String url = "jdbc:sqlserver://testuser:testpassword@localhost:33333/hreact";
		verifyURLWithPort( 33333, url, MSSQLConnectOptions.DEFAULT_PROPERTIES );
	}

	@Test
	public void testMSSQLUnsupportedProperties() {
		String url = "jdbc:sqlserver://testuser:testpassword@localhost:33333/hreact?prop_1=value_1";
		SqlConnectOptions connectOptions = verifyURLWithPort( 33333, url, MSSQLConnectOptions.DEFAULT_PROPERTIES );
		assertThat(connectOptions.getProperties().keySet().contains( "prop_1" )).isFalse();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMSSQLInvalidUrl() {
		String url = "jdbc:sqlserver://testuser:testpassword@localhost:33333/hreact%%%%xxxx=yyyyy";
		SqlConnectOptions connectOptions = verifyURLWithPort( 33333, url, MSSQLConnectOptions.DEFAULT_PROPERTIES );
		assertThat(connectOptions.getProperties().keySet().contains( "xxxx" )).isFalse();
	}

	@Test
	public void testMSSQLPassword() {
		String url = "jdbc:sqlserver://testuser:~!HReact!~@localhost/hreact";
		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getDatabase() ).isEqualTo( "hreact" );
		assertThat( connectOptions.getHost() ).isEqualTo( "localhost" );
		assertThat( connectOptions.getPort() ).isEqualTo( 1433 );
		assertThat( connectOptions.getUser() ).isEqualTo( "testuser" );
		assertThat( connectOptions.getPassword() ).isEqualTo( "~!HReact!~" );
	}

	// Example format https://vertx.io/docs/vertx-db2-client/java/#_configuration
	@Test
	public void testDB2WithUserInfo() {
		String url = "jdbc:db2://testuser:testpassword@localhost:44444/hreact:prop1=value1;";
		verifyURLWithPort( 44444, url, DB2ConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
	}

	@Test
	public void testDB2WithHostPort() {
		//jdbc:postgresql://localhost:49219/hreact?loggerLevel=OFF&user=hreact&password=hreact
		String url = "jdbc:db2://localhost:44444/hreact:user=testuser;password=testpassword;prop1=value1;";
		verifyURLWithPort( 44444, url, DB2ConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
	}

	@Test
	public void testDB2UnsupportedProperties() {
		String url = "jdbc:db2://testuser:testpassword@localhost:44444/hreact?prop_1=value_1";
		SqlConnectOptions connectOptions = verifyURLWithPort( 44444, url, DB2ConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
		assertThat(connectOptions.getProperties().keySet().contains( "prop_1" )).isFalse();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDB2InvalidUrl() {
		String url = "jdbc:db2://testuser:testpassword@localhost:44444/hreact%%%%xxxx=yyyyy";
		SqlConnectOptions connectOptions = verifyURLWithPort( 44444, url, DB2ConnectOptions.DEFAULT_CONNECTION_ATTRIBUTES );
		assertThat(connectOptions.getProperties().keySet().contains( "loggerLevel" )).isFalse();
	}

	@Test
	public void testDB2Password() {
		String url = "jdbc:db2://testuser:~!HReact!~@localhost/hreact";
		SqlConnectOptions connectOptions = getUriPoolConnectOptions( url );

		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getDatabase() ).isEqualTo( "hreact" );
		assertThat( connectOptions.getHost() ).isEqualTo( "localhost" );
		assertThat( connectOptions.getPort() ).isEqualTo( 50000 );
		assertThat( connectOptions.getUser() ).isEqualTo( "testuser" );
		assertThat( connectOptions.getPassword() ).isEqualTo( "~!HReact!~" );
	}
}

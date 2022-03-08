/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import org.hibernate.HibernateError;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;

import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import io.vertx.sqlclient.SqlConnectOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

public class JdbcUrlParserTest {
	private static final Map<String, String> EXTRA_PARAMS = new HashMap<>();
	static {
		EXTRA_PARAMS.put("param1","value1");
		EXTRA_PARAMS.put("param2","value2");
		EXTRA_PARAMS.put("param3","value3");
	}

	@Test
	public void returnsNullForNull() {
		assertThrows( HibernateError.class, () -> {
			URI uri = DefaultSqlClientPool.parse( null );
			assertThat( uri ).isNull();
		} );
	}

	@Test
	public void missingUser() {
		assertThrows( HibernateError.class, () -> {
			URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact" );
			DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
			final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
		} );
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

	@Test
	public void invalidProperty() {
		assertThrows( IllegalArgumentException.class, () -> {
			DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact?user=hello;$%_*(_(R=234" );
		} );
	}

	@Test
	public void loggerLevelParamRemovedFromUrl() { //TestContext context) {
		// There's currently a mismatch between
		String url = DatabaseConfiguration.getJdbcUrl();
		assertThat(url.contains( "loggerLevel" )).isFalse();
	}

	@Test
	public void extraParametersPostgreSQL() {
		testExtraParameters( "jdbc:postgresql://localhost:5432/hreact?user=hello", "&" );
	}

	@Test
	public void extraParametersDB2() {
		testExtraParameters( "jdbc:db2://localhost:50000/hreact:user=hello", ";" );
	}

	@Test
	public void extraParametersSqlServer() {
		testExtraParameters( "jdbc:sqlserver://localhost:1433;database=hreact;user=hello", ";" );
	}

	@Test
	public void extraParametersMySql() {
		testExtraParameters( "jdbc:mysql://localhost:3306/hreact?user=hello", "&" );
	}

	@Test
	public void extraParametersOracle() {
		testExtraParameters( "jdbc:oracle://localhost:1521/hreact?user=hello", "&" );
	}

	private void testExtraParameters(String baseUrl, String paramSeparator) {
		StringBuilder sb = new StringBuilder();
		for( String key : EXTRA_PARAMS.keySet() )  {
			sb.append( paramSeparator ).append( key ).append( "=" ).append( EXTRA_PARAMS.get( key ));
		}
		URI uri = DefaultSqlClientPool.parse( baseUrl + sb.toString() );
		DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
		final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
		assertThat( connectOptions ).isNotNull();
		assertThat( connectOptions.getProperties().get( "param1" ) ).isNotNull();
		assertThat( connectOptions.getProperties().get( "param2" ) ).isNotNull();
		assertThat( connectOptions.getProperties().get( "param3" ) ).isNotNull();
	}
}

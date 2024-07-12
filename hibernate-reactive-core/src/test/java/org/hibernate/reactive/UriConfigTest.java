/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.net.URI;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

@Timeout(value = 10, timeUnit = MINUTES)

public class UriConfigTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getUri() );
		configuration.setProperty( Settings.SQL_CLIENT_POOL_CONFIG, UriPoolConfiguration.class.getName() );
		return configuration;
	}

	@Test
	public void testUriConfig(VertxTestContext context) {
		test( context, getSessionFactory()
				.withSession( s -> s.createNativeQuery( selectQuery(), String.class ).getSingleResult() )
				.thenAccept( Assertions::assertNotNull )
		);
	}

	private String selectQuery() {
		switch ( dbType() ) {
			case POSTGRESQL:
			case COCKROACHDB:
			case SQLSERVER:
				return "select cast(current_timestamp as varchar)";
			case MARIA:
			case MYSQL:
				return "select cast(current_timestamp as char) from dual";
			case DB2:
				return "select cast(current time AS varchar) from sysibm.sysdummy1;";
			case ORACLE:
				return "select to_char(current_timestamp) from dual";
			default:
				throw new IllegalArgumentException( "Database not recognized: " + dbType().name() );
		}
	}

	/**
	 * This class is used by {@link UriConfigTest} to test that one can use a different implementation of
	 * {@link SqlClientPoolConfiguration}.
	 * <p>
	 * But, it also test {@link SqlConnectOptions#fromUri(String)} for each database.
	 *</p>
	 */
	public static class UriPoolConfiguration implements SqlClientPoolConfiguration {
		@Override
		public PoolOptions poolOptions() {
			return new PoolOptions();
		}

		@Override
		public SqlConnectOptions connectOptions(URI uri) {
			// For CockroachDB we use the PostgreSQL Vert.x client
			String uriString = uri.toString().replaceAll( "^cockroach(db)?:", "postgres:" );
			if ( uriString.startsWith( "sqlserver" ) ) {
				// Testscontainer adds encrypt=false to the url. The problem is that it uses the JDBC syntax that's
				// different from the supported one by the Vert.x driver.
				uriString = uriString.replaceAll( ";[e|E]ncrypt=false", "?encrypt=false" );
			}
			return SqlConnectOptions.fromUri( uriString );
		}
	}
}

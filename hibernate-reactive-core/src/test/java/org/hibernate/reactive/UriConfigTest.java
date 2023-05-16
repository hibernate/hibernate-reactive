/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;

public class UriConfigTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Class<? extends Dialect> dialect;
		switch ( dbType() ) {
			case POSTGRESQL: dialect = PostgreSQLDialect.class; break;
			case COCKROACHDB: dialect = CockroachDialect.class; break;
			case MYSQL: dialect = MySQLDialect.class; break;
			case MARIA: dialect = MariaDBDialect.class; break;
			case SQLSERVER: dialect = SQLServerDialect.class; break;
			case DB2: dialect = DB2Dialect.class; break;
			case ORACLE: dialect = OracleDialect.class; break;
			default: throw new IllegalArgumentException( "Database not recognized: " + dbType().name() );
		}

		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Environment.DIALECT, dialect.getName() );
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
}

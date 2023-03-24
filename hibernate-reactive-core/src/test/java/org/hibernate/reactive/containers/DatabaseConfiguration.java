/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SQLServerDialect;

/**
 * Contains the common constants that we need for the configuration of databases
 * during tests.
 */
public class DatabaseConfiguration {

	public static final boolean USE_DOCKER = Boolean.getBoolean("docker");

	public enum DBType {
		DB2( DB2Database.INSTANCE, 50000, "com.ibm.db2.jcc.DB2Driver", DB2Dialect.class ),
		MYSQL( MySQLDatabase.INSTANCE, 3306, "com.mysql.cj.jdbc.Driver", MySQLDialect.class ),
		MARIA( MariaDatabase.INSTANCE, 3306, "org.mariadb.jdbc.Driver", MariaDBDialect.class, "mariadb" ),
		POSTGRESQL( PostgreSQLDatabase.INSTANCE, 5432, "org.postgresql.Driver", PostgreSQLDialect.class, "POSTGRES", "PG" ),
		COCKROACHDB( CockroachDBDatabase.INSTANCE, 26257, "org.postgresql.Driver", CockroachDialect.class, "COCKROACH" ),
		SQLSERVER( MSSQLServerDatabase.INSTANCE, 1433, "com.microsoft.sqlserver.jdbc.SQLServerDriver", SQLServerDialect.class, "MSSQL", "MSSQLSERVER" ),
		ORACLE( OracleDatabase.INSTANCE, 1521, "oracle.jdbc.OracleDriver", OracleDialect.class );

		private final TestableDatabase configuration;
		private final int defaultPort;

		// A list of alternative names that can be used to select the db
		private final String[] aliases;

		// JDBC Configuration for when we need to compare what we are doing with ORM
		private final String jdbcDriver;

		private final Class<? extends Dialect> dialect;

		DBType(TestableDatabase configuration, int defaultPort, String jdbcDriver, Class<? extends Dialect> dialect, String... aliases) {
			this.configuration = configuration;
			this.defaultPort = defaultPort;
			this.aliases = aliases;
			this.dialect = dialect;
			this.jdbcDriver = jdbcDriver;
		}

		@Override
		public String toString() {
			// Print the name and all the aliases
			StringBuilder result = new StringBuilder( name() );
			Stream.of( aliases ).forEach( alt -> result.append( ", " ).append( alt ) );
			return result.toString();
		}

		public static DBType fromString(String dbName) {
			Objects.requireNonNull( dbName );
			for ( DBType dbType : values() ) {
				// Look at the enum name
				if ( dbName.equalsIgnoreCase( dbType.name() ) ) {
					return dbType;
				}
				// Search in the aliases
				for ( String alias : dbType.aliases ) {
					if ( alias.equalsIgnoreCase( dbName ) ) {
						return dbType;
					}
				}
			}
			throw new IllegalArgumentException( "Unknown DB type '" + dbName + "' specified. Allowed values are: " + Arrays
					.toString( DBType.values() ) );
		}

		public int getDefaultPort() {
			return defaultPort;
		}

		public String getJdbcDriver() {
			return jdbcDriver;
		}

		public Class<? extends Dialect> getDialectClass() {
			return dialect;
		}
	}

	public static final String USERNAME = "hreact";
	public static final String PASSWORD = "hreact";
	public static final String DB_NAME = "hreact";

	private static DBType dbType;

	public static DBType dbType() {
		if (dbType == null) {
			String dbTypeString = System.getProperty( "db", DBType.POSTGRESQL.name() );
			dbType = DBType.fromString( dbTypeString );
			System.out.println( "Using database type: " + dbType.name() );
		}
		return dbType;
	}

 	public static String getJdbcUrl() {
		return dbType().configuration.getJdbcUrl();
	}

	public static String getUri() {
		return dbType().configuration.getUri();
	}

	public static String createJdbcUrl(String host, int port, String database, Map<String, String> properties) {
		return dbType().configuration.createJdbcUrl( host, port, database, properties );
	}

	public static String getDatatypeQuery(String tableName, String columnName) {
		return dbType().configuration.getNativeDatatypeQuery( tableName, columnName );
	}

	public static String expectedDatatype(Class<?> dataType) {
		return dbType().configuration.getExpectedNativeDatatype( dataType );
	}

	private DatabaseConfiguration() {
	}

}

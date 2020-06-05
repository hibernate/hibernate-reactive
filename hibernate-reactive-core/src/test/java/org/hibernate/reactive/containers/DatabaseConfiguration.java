/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Contains the common constants that we need for the configuration of databases
 * during tests.
 */
public class DatabaseConfiguration {

	public static final boolean USE_DOCKER = Boolean.getBoolean("docker");

	public static enum DBType {
		DB2( DB2Database.INSTANCE ),
		MYSQL( MySQLDatabase.INSTANCE ),
		POSTGRESQL( PostgreSQLDatabase.INSTANCE, "POSTGRES", "PG" );

		private final TestableDatabase configuration;

		// A list of alternative names that can be used to select the db
		private final String[] aliases;

		DBType(TestableDatabase configuration, String... aliases) {
			this.configuration = configuration;
			this.aliases = aliases;
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

	/**
	 * Builds a prepared statement SQL string in a portable way. For example,
	 * DB2 and MySQL use syntax like "SELECT * FROM FOO WHERE BAR = ?" but
	 * PostgreSQL uses syntax like "SELECT * FROM FOO WHERE BAR = $1"
	 * @param parts The parts of the SQL not including the parameter tokens. For example:
	 * <code>statement("SELECT * FROM FOO WHERE BAR = ", "")</code>
	 */
	public static String statement(String... parts) {
		return dbType().configuration.statement( parts );
	}

	private DatabaseConfiguration() {
	}

}

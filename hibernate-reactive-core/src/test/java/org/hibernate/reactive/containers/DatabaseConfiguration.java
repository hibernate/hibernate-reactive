/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.Arrays;

/**
 * Contains the common constants that we need for the configuration of databases
 * during tests.
 */
public class DatabaseConfiguration {

	public static final boolean USE_DOCKER = Boolean.getBoolean("docker");

	public static enum DBType {
		DB2,
		MYSQL,
		POSTGRESQL
	}

	public static final String USERNAME = "hreact";
	public static final String PASSWORD = "hreact";
	public static final String DB_NAME = "hreact";

	private static DBType dbType;

	public static DBType dbType() {
		if (dbType == null) {
			String dbTypeString = System.getProperty( "db", DBType.POSTGRESQL.name() ).toUpperCase();
			if ( "PG".equals( dbTypeString ) || "POSTGRES".equals( dbTypeString ) ) {
				dbType = DBType.POSTGRESQL;
			} else {
				try {
					dbType = DBType.valueOf( dbTypeString );
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException( "Unknown DB type '" + dbTypeString +
							"' specified. Allowed values are: " + Arrays.toString( DBType.values() ), e );
				}
			}
			System.out.println( "Using database type: " + dbType );
		}
		return dbType;
	}

	public static String getJdbcUrl() {
		DBType dbType = dbType();
		switch (dbType) {
			case DB2:
				return DB2Database.getJdbcUrl();
			case MYSQL:
				return MySQLDatabase.getJdbcUrl();
			case POSTGRESQL:
				return PostgreSQLDatabase.getJdbcUrl();
			default:
				throw new IllegalArgumentException( "Unknown DB type: "+ dbType );
		}
	}

	/**
	 * Builds a prepared statement SQL string in a portable way. For example,
	 * DB2 and MySQL use syntax like "SELECT * FROM FOO WHERE BAR = ?" but
	 * PostgreSQL uses syntax like "SELECT * FROM FOO WHERE BAR = $1"
	 * @param parts The parts of the SQL not including the parameter tokens. For example:
	 * <code>statement("SELECT * FROM FOO WHERE BAR = ", "")</code>
	 */
	public static String statement(String... parts) {
		DBType dbType = dbType();
		switch (dbType) {
			case DB2:
			case MYSQL:
				return String.join("?", parts);
			case POSTGRESQL:
			    StringBuilder sb = new StringBuilder();
			    for (int i = 0; i < parts.length; i++) {
			      if (i > 0) {
			        sb.append("$").append((i));
			      }
			      sb.append(parts[i]);
			    }
			    return sb.toString();
			default:
				throw new IllegalArgumentException( "Unknown DB type: "+ dbType );
		}
	}

	private DatabaseConfiguration() {
	}

}

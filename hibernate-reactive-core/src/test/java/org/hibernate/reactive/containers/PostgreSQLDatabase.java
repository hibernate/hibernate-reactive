/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.PostgreSQLContainer;

class PostgreSQLDatabase implements TestableDatabase {

	public static PostgreSQLDatabase INSTANCE = new PostgreSQLDatabase();

	public final static String IMAGE_NAME = "postgres:13.0";

	/**
	 * Holds configuration for the PostgreSQL database contianer. If the build is run with <code>-Pdocker</code> then
	 * Testcontianers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>( IMAGE_NAME )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	private String getRegularJdbcUrl() {
		return "jdbc:postgresql://localhost:5432/" + postgresql.getDatabaseName() + "?loggerLevel=OFF";
	}

	@Override
	public String getJdbcUrl() {
		String address;
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			postgresql.start();
			address = postgresql.getJdbcUrl();
		}
		else {
			address = getRegularJdbcUrl();
		}
		return buildJdbcUrlWithCredentials( address );
	}

	@Override
	public String getUri() {
		String address;
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			postgresql.start();
			address = postgresql.getJdbcUrl();
		}
		else {
			address = getRegularJdbcUrl();
		}
		return buildUriWithCredentials( address );
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "&user=" + postgresql.getUsername() + "&password=" + postgresql.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "postgresql://" + postgresql.getUsername() + ":" + postgresql.getPassword() + "@" + jdbcUrl.substring(18);
	}

	private PostgreSQLDatabase() {
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.CockroachContainer;

class CockroachDBDatabase implements TestableDatabase {

	public static CockroachDBDatabase INSTANCE = new CockroachDBDatabase();

	public final static String IMAGE_NAME = "cockroachdb/cockroach:v20.2.5";

	/**
	 * Holds configuration for the CockroachDB database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final CockroachContainer cockroachDb = new CockroachContainer( IMAGE_NAME ).withReuse( true );

	private String getRegularJdbcUrl() {
		return "jdbc:postgresql://localhost:26257/" + cockroachDb.getDatabaseName();
	}

	@Override
	public String getJdbcUrl() {
		String address;
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			cockroachDb.start();
			address = cockroachDb.getJdbcUrl();
		}
		else {
			address = getRegularJdbcUrl();
		}
		return buildJdbcUrlWithCredentials( address + "?sslmode=disable" );
	}

	@Override
	public String getUri() {
		String address;
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			cockroachDb.start();
			address = cockroachDb.getJdbcUrl();
		}
		else {
			address = getRegularJdbcUrl();
		}
		return buildUriWithCredentials( address + "?sslmode=disable" );
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "&user=" + cockroachDb.getUsername() + "&password=" + cockroachDb.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "postgresql://" + cockroachDb.getUsername() + "@" + jdbcUrl.substring(18);
	}

	private CockroachDBDatabase() {
	}
}

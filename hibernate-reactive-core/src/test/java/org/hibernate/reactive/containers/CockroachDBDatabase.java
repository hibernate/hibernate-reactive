/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.hibernate.HibernateException;

import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.shaded.org.apache.commons.lang.StringUtils.isNotBlank;

class CockroachDBDatabase extends PostgreSQLDatabase {

	public static CockroachDBDatabase INSTANCE = new CockroachDBDatabase();

	public static final String IMAGE_NAME = "cockroachdb/cockroach";
	public static final String IMAGE_VERSION = ":v21.2.4";

	/**
	 * Holds configuration for the CockroachDB database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final CockroachContainer cockroachDb = new CockroachContainer(
			DockerImageName.parse( DOCKER_REPOSITORY + IMAGE_NAME + IMAGE_VERSION ).asCompatibleSubstituteFor( IMAGE_NAME ) )
			// Username, password and database are not supported by test container at the moment
			// Testcontainers will use a database named 'postgres' and the 'root' user
			.withReuse( true );

	private String getRegularJdbcUrl() {
		return "jdbc:postgres://localhost:26257/" + cockroachDb.getDatabaseName();
	}

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() )
				.replaceAll( "^jdbc:postgre(s|sql):", "jdbc:cockroachdb:" );
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() )
				.replaceAll( "^postgre(s|sql):", "jdbc:cockroachdb:" );
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			cockroachDb.start();
			enableTemporaryTables();
			return disableSslMode( cockroachDb.getJdbcUrl() );
		}

		return disableSslMode( getRegularJdbcUrl() );
	}

	private static String disableSslMode(String url) {
		return url + "?sslmode=disable";
	}

	/**
	 * Temporary tables support is experimental but we need it when updating entities in a hierarchy
	 */
	private static void enableTemporaryTables() {
		runSql( "SET CLUSTER SETTING sql.defaults.experimental_temporary_tables.enabled = 'true';" );
	}

	private static void runSql(String command) {
		Container.ExecResult execResult;
		try {
			execResult = cockroachDb.execInContainer(
					"sh",
					"-c",
					"./cockroach sql --insecure -e \"" + command + "\""
			);
		}
		catch (Exception e) {
			throw new HibernateException( "[CockroachDB] Error running " + command, e );
		}
		if ( execResult != null && execResult.getExitCode() != 0 ) {
			String error = isNotBlank( execResult.getStderr() )
					? execResult.getStderr()
					: execResult.getStdout();
			throw new HibernateException( "[CockroachDB] Error running " + command + " [" + execResult.getExitCode() + "]: " + error );
		}
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "&user=" + cockroachDb.getUsername();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		String suffix = jdbcUrl.replaceAll( "^jdbc:postgre(s|sql)://", "" );
		return "postgresql://" + cockroachDb.getUsername() + "@" + suffix;
	}

	private CockroachDBDatabase() {
	}

}

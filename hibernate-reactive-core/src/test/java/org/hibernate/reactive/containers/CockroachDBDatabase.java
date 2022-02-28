/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.hibernate.HibernateException;

import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.Container;

import static org.testcontainers.shaded.org.apache.commons.lang.StringUtils.isNotBlank;

class CockroachDBDatabase extends PostgreSQLDatabase {

	public static CockroachDBDatabase INSTANCE = new CockroachDBDatabase();

	public final static String IMAGE_NAME = "cockroachdb/cockroach:v21.2.4";

	/**
	 * Holds configuration for the CockroachDB database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final CockroachContainer cockroachDb = new CockroachContainer( IMAGE_NAME )
			// Username, password and database are not supported by test container at the moment
			// Testcontainers will use a database named 'postgres' and the 'root' user
			.withReuse( true );

	@Override
	public String getDefaultUrl() {
		return "cockroachdb://" + TestableDatabase.credentials( cockroachDb ) + "localhost:26257/" + cockroachDb.getDatabaseName();
	}

	@Override
	public String getConnectionUri() {
		String vertxUri = disableSslMode( connectionUri( cockroachDb ) )
				.replaceAll( "^postgre(s|sql):", "cockroachdb:" );
		if ( DatabaseConfiguration.USE_DOCKER ) {
			enableTemporaryTables();
		}
		return vertxUri;
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

	private CockroachDBDatabase() {
	}
}

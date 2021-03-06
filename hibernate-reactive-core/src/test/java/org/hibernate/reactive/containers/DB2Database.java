/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.Db2Container;

class DB2Database implements TestableDatabase {

	public static DB2Database INSTANCE = new DB2Database();

	public final static String IMAGE_NAME = "ibmcom/db2:11.5.4.0";

	/**
	 * Holds configuration for the DB2 database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	static final Db2Container db2 = new Db2Container( IMAGE_NAME )
		      .withUsername(DatabaseConfiguration.USERNAME)
		      .withPassword(DatabaseConfiguration.PASSWORD)
		      .withDatabaseName(DatabaseConfiguration.DB_NAME)
		      .acceptLicense()
		      .withReuse(true);

	private String getRegularJdbcUrl() {
		return "jdbc:db2://localhost:50000/" + db2.getDatabaseName();
	}

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			db2.start();
			return db2.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ":user=" + db2.getUsername() + ";password=" + db2.getPassword() + ";";
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "db2://" + db2.getUsername() + ":" + db2.getPassword() + "@" + jdbcUrl.substring(11);
	}

	private DB2Database() {
	}

}

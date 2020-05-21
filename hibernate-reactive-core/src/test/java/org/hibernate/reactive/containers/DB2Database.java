package org.hibernate.reactive.containers;

import org.testcontainers.containers.Db2Container;

class DB2Database {

	public final static String IMAGE_NAME = "ibmcom/db2:11.5.0.0a";

	/**
	 * Holds configuration for the DB2 database contianer. If the build is run with <code>-Pdocker</code> then
	 * Testcontianers+Docker will be used.
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

	public static String getJdbcUrl() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			db2.start();
			return buildJdbcUrlWithCredentials( db2.getJdbcUrl() );
		}
		else {
			return buildJdbcUrlWithCredentials( "jdbc:db2://localhost:50000/" + db2.getDatabaseName() );
		}
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ":user=" + db2.getUsername() + ";password=" + db2.getPassword() + ";";
	}

	private DB2Database() {
	}
	
}

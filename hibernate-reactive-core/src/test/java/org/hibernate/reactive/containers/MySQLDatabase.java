package org.hibernate.reactive.containers;

public class MySQLDatabase {

	/**
	 * Holds configuration for the MySQL database contianer. If the build is run with <code>-Pdocker</code> then
	 * Testcontianers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final VertxMySqlContainer mysql = new VertxMySqlContainer()
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	public static String getJdbcUrl() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			mysql.start();
			return buildUrlWithCredentials( mysql.getJdbcUrl() );
		}
		else {
			return buildUrlWithCredentials( "jdbc:mysql://localhost:3306/" + mysql.getDatabaseName() );
		}
	}

	static String buildUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "?user=" + mysql.getUsername() + "&password=" + mysql.getPassword() + "&serverTimezone=UTC";
	}

	private MySQLDatabase() {
	}

}

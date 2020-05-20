package org.hibernate.reactive.containers;

import org.testcontainers.containers.PostgreSQLContainer;

public class PostgreSQLDatabase {

	public final static String IMAGE_NAME = "postgres:12-alpine";

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

	public static String getJdbcUrl() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			postgresql.start();
			return buildJdbcUrlWithCredentials( postgresql.getJdbcUrl() );
		}
		else {
			return buildJdbcUrlWithCredentials( "jdbc:postgresql://localhost:5432/" + postgresql.getDatabaseName() + "?loggerLevel=OFF" );
		}
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "&user=" + postgresql.getUsername() + "&password=" + postgresql.getPassword();
	}

	private PostgreSQLDatabase() {
	}

}

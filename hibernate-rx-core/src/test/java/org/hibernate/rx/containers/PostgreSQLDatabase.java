package org.hibernate.rx.containers;

import org.testcontainers.containers.PostgreSQLContainer;

public class PostgreSQLDatabase {

	private static final boolean USE_DOCKER = Boolean.getBoolean( "docker" );

	/**
	 * Holds configuration for the PostgreSQL database contianer. If the build is run with <code>-Pdocker</code> then
	 * Testcontianers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>()
			.withUsername( "hibernate-rx" )
			.withPassword( "hibernate-rx" )
			.withDatabaseName( "hibernate-rx" )
			.withReuse( true );

	public static String getJdbcUrl() {
		if ( USE_DOCKER ) {
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

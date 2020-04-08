package org.hibernate.rx.containers;

import org.testcontainers.containers.MySQLContainer;

public class MySQLDatabase {

	private static final boolean USE_DOCKER = Boolean.getBoolean( "docker" );

	/**
	 * Holds configuration for the MySQL database contianer. If the build is run with <code>-Pdocker</code> then
	 * Testcontianers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final MySQLContainer<?> mysql = new MySQLContainer<>()
			.withUsername( "hibernate-rx" )
			.withPassword( "hibernate-rx" )
			.withDatabaseName( "hibernate-rx" )
			.withReuse( true );

	public static String getJdbcUrl() {
		if ( USE_DOCKER ) {
			mysql.start();
			return buildJdbcUrlWithCredentials( mysql.getJdbcUrl() );
		}
		else {
			return buildJdbcUrlWithCredentials( "jdbc:mysql://localhost:3306/" + mysql.getDatabaseName() );
		}
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "?user=" + mysql.getUsername() + "&password=" + mysql.getPassword() + "&serverTimezone=UTC";
	}

	private MySQLDatabase() {
	}

}

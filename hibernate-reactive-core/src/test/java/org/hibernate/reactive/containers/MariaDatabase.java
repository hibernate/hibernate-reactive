/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.testcontainers.containers.MariaDBContainer;

import static org.hibernate.reactive.containers.DockerImage.imageName;

class MariaDatabase extends MySQLDatabase {

	private static final Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {{
		expectedDBTypeForClass.putAll( MySQLDatabase.expectedDBTypeForClass );

		// Even if the column is created using `json`, the client will return `longtext` as the type.
		expectedDBTypeForClass.put( BigDecimal[].class, "longtext" );
		expectedDBTypeForClass.put( BigInteger[].class, "longtext" );
	}}

	static MariaDatabase INSTANCE = new MariaDatabase();

	/**
	 * Holds configuration for the MariaDB database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final MariaDBContainer<?> maria = new MariaDBContainer<>( imageName( "mariadb", "11.4.2" ) )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	private String getRegularJdbcUrl() {
		return "jdbc:mariadb://localhost:3306/" + maria.getDatabaseName();
	}

	@Override
	public String getExpectedNativeDatatype(Class<?> dataType) {
		return expectedDBTypeForClass.get( dataType );
	}

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	@Override
	public String getScheme() {
		return "mariadb:";
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			maria.start();
			return maria.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "?user=" + maria.getUsername() + "&password=" + maria.getPassword() + "&serverTimezone=UTC";
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "mariadb://" + maria.getUsername() + ":" + maria.getPassword() + "@"
				+ jdbcUrl.substring( "jdbc:mariadb://".length() );
	}

	private MariaDatabase() {
	}

}

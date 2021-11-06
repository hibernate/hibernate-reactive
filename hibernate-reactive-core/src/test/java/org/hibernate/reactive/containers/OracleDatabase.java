/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.OracleContainer;

class OracleDatabase implements TestableDatabase {

	public static final String IMAGE_NAME = "gvenzl/oracle-xe:21.3.0-full";

	public static final OracleDatabase INSTANCE = new OracleDatabase();

	public static final OracleContainer oracle = new OracleContainer( IMAGE_NAME )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	private String getRegularJdbcUrl() {
		return "jdbc:oracle:thin:@//localhost:1521";
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	@Override
	public String getExpectedNativeDatatype(Class<?> dataType) {
		throw new UnsupportedOperationException();
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			oracle.start();
			return oracle.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	private String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ";user=" + oracle.getUsername() + ";password=" + oracle.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "oracle://" + oracle.getUsername() + ":" + oracle.getPassword() + "@" + jdbcUrl.substring( "jdbc:oracle://".length() );
	}

	private OracleDatabase() {
	}

}

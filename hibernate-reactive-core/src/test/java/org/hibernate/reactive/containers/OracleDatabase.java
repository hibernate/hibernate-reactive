/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.OracleContainer;

/**
 * Connection string for Oracle thin should be something like:
 *
 * jdbc:oracle:thin:[<user>/<password>]@<host>[:<port>]:<SID>
 */
class OracleDatabase implements TestableDatabase {

	public static final String IMAGE_NAME = "gvenzl/oracle-xe:18-slim";

	public static final OracleDatabase INSTANCE = new OracleDatabase();

	public static final OracleContainer oracle = new OracleContainer( IMAGE_NAME )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withLogConsumer( of -> System.out.println( of.getUtf8String() ) )
			.withReuse( true );

	@Override
	public String getJdbcUrl() {
		return addCredentials( address() );
	}

	private String getRegularJdbcUrl() {
		// This should be jdbc:oracle:thin:[<user>/<password>]@<host>[:<port>]:<SID>
		return "jdbc:oracle:thin:@localhost:1521/hreact";
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

	private static String addCredentials(String jdbcUrl) {
		// Note that this is not supported by the Oracle JDBC Driver
		return jdbcUrl + "?user=" + oracle.getUsername() + "&password=" + oracle.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		final String url = addCredentials( jdbcUrl );
		if ( url.startsWith( "jdbc:" ) ) {
			return jdbcUrl.substring( "jdbc:".length() );
		}
		return jdbcUrl;
	}

	private OracleDatabase() {
	}

}

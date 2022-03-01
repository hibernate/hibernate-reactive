/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * A database that we use for testing.
 */
public interface TestableDatabase {

	String getConnectionUri();

	default String connectionUri(JdbcDatabaseContainer<?> container) {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			container.start();
			return TestableDatabase.vertxUrl( container );
		}

		return getDefaultUrl();
	}

	String getDefaultUrl();

	static String vertxUrl(JdbcDatabaseContainer container) {
		String vertxUrl = container.getJdbcUrl().replaceAll( "^jdbc:", "" );
		int start = vertxUrl.indexOf( ":" ) + 1;
		String scheme = vertxUrl.substring( 0, start );
		vertxUrl = vertxUrl.substring( start ).replaceAll( "^//", "" );
		final String uri = scheme + "//" + credentials( container ) + vertxUrl;
		return uri;
	}

	static String credentials(JdbcDatabaseContainer<?> container) {
		return container.getUsername() + ":" + container.getPassword() + "@";
	}

	default String getNativeDatatypeQuery(String tableName, String columnName) {
		return "select data_type from information_schema.columns where table_name = '" + tableName + "' and column_name = '" + columnName + "'";
	}

	String getExpectedNativeDatatype(Class<?> dataType);
}

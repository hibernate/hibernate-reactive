/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.Map;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * A database that we use for testing.
 */
public interface TestableDatabase {

	String getJdbcUrl();

	String getUri();

 	/**
	 * @return the database scheme for the connection. Example: {@code mysql:}
	 */
	default String getScheme() {
		return dbType().name().toLowerCase() + ":";
	}

	default String getNativeDatatypeQuery(String tableName, String columnName) {
		return "select data_type from information_schema.columns where table_name = '" + tableName + "' and column_name = '" + columnName + "'";
	}

	String getExpectedNativeDatatype(Class<?> dataType);

	default String createJdbcUrl(String host, int port, String database, Map<String, String> params) {
		final StringBuilder paramsBuilder = new StringBuilder();
		if ( params != null && !params.isEmpty() ) {
			params.forEach( (key, value) -> {
				paramsBuilder.append( jdbcParamDelimiter() );
				paramsBuilder.append( key );
				paramsBuilder.append( "=" );
				paramsBuilder.append( value );
			} );
		}
		String url = "jdbc:" + getScheme() + "//" + host;
		if ( port > -1 ) {
			url += ":" + port;
		}
		url += "/";
		if ( database != null ) {
			url += database;
		}
		if ( !paramsBuilder.isEmpty() ) {
			url += jdbcStartQuery() + paramsBuilder.substring( 1 );
		}
		return url;
	}

	default String jdbcStartQuery() {
		return "?";
	}

	default String jdbcParamDelimiter() {
		return "&";
	}
}

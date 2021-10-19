/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.EnumMap;
import java.util.Map;

class MySQLDatabase implements TestableDatabase {

	static MySQLDatabase INSTANCE = new MySQLDatabase();

	public final static String IMAGE_NAME = "mysql:8.0.27";

	String findTypeForColumnBaseQuery =
					"select data_type from information_schema.columns where table_name = '" + TABLE_PARAM + "'  and column_name = '" + COLUMN_PARAM + "'";

	public static Map<DataType, String> expectedDBTypeForEntityType = new EnumMap<DataType, String>( DataType.class);
	static {{
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_PRIMATIVE, "boolean");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_FIELD, "boolean");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_NUMERIC, "integer");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_TRUE_FALSE, "character");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_YES_NO, "character");
		expectedDBTypeForEntityType.put( DataType.INT_PRIMATIVE, "integer");
		expectedDBTypeForEntityType.put( DataType.INTEGER_FIELD, "integer");
		expectedDBTypeForEntityType.put( DataType.LONG_PRIMATIVE, "bigint");
		expectedDBTypeForEntityType.put( DataType.LONG_FIELD, "bigint");
		expectedDBTypeForEntityType.put( DataType.FLOAT_PRIMATIVE, "real");
		expectedDBTypeForEntityType.put( DataType.FLOAT_FIELD, "real");
		expectedDBTypeForEntityType.put( DataType.DOUBLE_PRIMATIVE, "double precision");
		expectedDBTypeForEntityType.put( DataType.DOUBLE_FIELD, "double precision");
		expectedDBTypeForEntityType.put( DataType.BYTE_PRIMATIVE, "smallint");
		expectedDBTypeForEntityType.put( DataType.BYTE_FIELD, "smallint");
		expectedDBTypeForEntityType.put( DataType.BYTES_PRIMATIVE, "bytea");
		expectedDBTypeForEntityType.put( DataType.URL, "character varying");
		expectedDBTypeForEntityType.put( DataType.TIMEZONE, "character varying");
		expectedDBTypeForEntityType.put( DataType.DATE_TEMPORAL_TYPE, "date");
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIMESTAMP_TEMPORAL_TYPE, "timestamp without time zone");
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIME_TEMPORAL_TYPE, "time without time zone");
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_DATE_TEMPORAL_TYPE, "date");
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_TIMESTAMP_TEMPORAL_TYPE, "timestamp without time zone");
		expectedDBTypeForEntityType.put( DataType.LOCALDATE, "date");
		expectedDBTypeForEntityType.put( DataType.LOCALTIME, "time without time zone");
		expectedDBTypeForEntityType.put( DataType.LOCALDATETIME, "timestamp without time zone");
		expectedDBTypeForEntityType.put( DataType.BIGINTEGER, "numeric");
		expectedDBTypeForEntityType.put( DataType.BIGDECIMAL, "numeric");
		expectedDBTypeForEntityType.put( DataType.SERIALIZABLE, "bytea");
		expectedDBTypeForEntityType.put( DataType.UUID, "uuid");
		expectedDBTypeForEntityType.put( DataType.INSTANT, "timestamp without time zone");
		expectedDBTypeForEntityType.put( DataType.DURATION, "bigint");
		expectedDBTypeForEntityType.put( DataType.CHARACTER, "character");
		expectedDBTypeForEntityType.put( DataType.TEXT, "text");
		expectedDBTypeForEntityType.put( DataType.STRING, "character varying");
		expectedDBTypeForEntityType.put( DataType.SERIALIZABLE, "bytea");
//		pgExpectedResultsMap.put( DataType.BLOB, "oid");
	}};

	/**
	 * Holds configuration for the MySQL database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final VertxMySqlContainer mysql = new VertxMySqlContainer( IMAGE_NAME )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	private String getRegularJdbcUrl() {
		return "jdbc:mysql://localhost:3306/" + mysql.getDatabaseName();
	}

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	@Override
	public String getDatatypeQuery(String tableName, String columnName) {
		return findTypeForColumnBaseQuery.replace(
				TABLE_PARAM, tableName).replace(
				COLUMN_PARAM, columnName);
	}

	@Override
	public String getExpectedDatatype(DataType dataType) {
		return expectedDBTypeForEntityType.get(dataType);
	}

	private String address() {
		String address;
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			mysql.start();
			address = mysql.getJdbcUrl();
		}
		else {
			address = getRegularJdbcUrl();
		}
		return address;
	}

	static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "?user=" + mysql.getUsername() + "&password=" + mysql.getPassword() + "&serverTimezone=UTC";
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "mysql://" + mysql.getUsername() + ":" + mysql.getPassword() + "@" + jdbcUrl.substring(13);
	}

	protected MySQLDatabase() {
	}

}

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
					"select DATA_TYPE from information_schema.columns where TABLE_NAME = '" + TABLE_PARAM + "'  and COLUMN_NAME = '" + COLUMN_PARAM + "'";

	String selectColumnsOnlyQuery
			= "select COLUMN_NAME from information_schema.columns where TABLE_NAME = '" + TABLE_PARAM + "'";

	public static Map<DataType, String> expectedDBTypeForEntityType = new EnumMap<DataType, String>( DataType.class);
	static {{
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_PRIMITIVE, "bit" );
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_FIELD, "bit" );
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_NUMERIC, "int" );
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_TRUE_FALSE, "char" );
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_YES_NO, "char" );
		expectedDBTypeForEntityType.put( DataType.INT_PRIMITIVE, "int" );
		expectedDBTypeForEntityType.put( DataType.INTEGER_FIELD, "int" );
		expectedDBTypeForEntityType.put( DataType.LONG_PRIMITIVE, "bigint" );
		expectedDBTypeForEntityType.put( DataType.LONG_FIELD, "bigint" );
		expectedDBTypeForEntityType.put( DataType.FLOAT_PRIMITIVE, "float" );
		expectedDBTypeForEntityType.put( DataType.FLOAT_FIELD, "float" );
		expectedDBTypeForEntityType.put( DataType.DOUBLE_PRIMITIVE, "double" );
		expectedDBTypeForEntityType.put( DataType.DOUBLE_FIELD, "double" );
		expectedDBTypeForEntityType.put( DataType.BYTE_PRIMITIVE, "tinyint" );
		expectedDBTypeForEntityType.put( DataType.BYTE_FIELD, "tinyint" );
		expectedDBTypeForEntityType.put( DataType.BYTES_PRIMITIVE, "tinyblob" );
		expectedDBTypeForEntityType.put( DataType.URL, "varchar" );
		expectedDBTypeForEntityType.put( DataType.TIMEZONE, "varchar" );
		expectedDBTypeForEntityType.put( DataType.DATE_TEMPORAL_TYPE, "date" );
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIMESTAMP_TEMPORAL_TYPE, "datetime" );
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIME_TEMPORAL_TYPE, "time" );
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_DATE_TEMPORAL_TYPE, "date" );
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_TIMESTAMP_TEMPORAL_TYPE, "datetime" );
		expectedDBTypeForEntityType.put( DataType.LOCALDATE, "date" );
		expectedDBTypeForEntityType.put( DataType.LOCALTIME, "time" );
		expectedDBTypeForEntityType.put( DataType.LOCALDATETIME, "datetime" );
		expectedDBTypeForEntityType.put( DataType.BIGINTEGER, "decimal" );
		expectedDBTypeForEntityType.put( DataType.BIGDECIMAL, "decimal" );
		expectedDBTypeForEntityType.put( DataType.SERIALIZABLE, "tinyblob" );
		expectedDBTypeForEntityType.put( DataType.UUID, "binary" );
		expectedDBTypeForEntityType.put( DataType.INSTANT, "datetime" );
		expectedDBTypeForEntityType.put( DataType.DURATION, "bigint" );
		expectedDBTypeForEntityType.put( DataType.CHARACTER, "char" );
		expectedDBTypeForEntityType.put( DataType.TEXT, "text" );
		expectedDBTypeForEntityType.put( DataType.STRING, "varchar" );
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
	public String getNativeDatatypeQuery(String tableName, String columnName) {
		if( columnName == null ) {
			return selectColumnsOnlyQuery.replace(TABLE_PARAM, tableName );
		}
		return findTypeForColumnBaseQuery.replace(
				TABLE_PARAM, tableName).replace(
				COLUMN_PARAM, columnName);
	}

	@Override
	public String getExpectedNativeDatatype(DataType dataType) {
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

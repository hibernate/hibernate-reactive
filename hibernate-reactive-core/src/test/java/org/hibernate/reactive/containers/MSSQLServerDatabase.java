/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.EnumMap;
import java.util.Map;

import org.testcontainers.containers.MSSQLServerContainer;

/**
 * The JDBC driver syntax is:
 *     jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]
 *
 * But the Vert.x SQL client syntax is:
 *     sqlserver://[user[:[password]]@]host[:port][/database][?attribute1=value1&attribute2=value2…​]
 */
class MSSQLServerDatabase implements TestableDatabase {

	public static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2019-latest";

	public static final MSSQLServerDatabase INSTANCE = new MSSQLServerDatabase();

	public static final String PASSWORD = "~!HReact!~";

	String findTypeForColumnBaseQuery =
			"select data_type from information_schema.columns where table_name = '" + TABLE_PARAM + "' and column_name = '" + COLUMN_PARAM + "'";


	public static Map<DataType, String> expectedDBTypeForEntityType = new EnumMap<DataType, String>( DataType.class);
	static {{
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_PRIMATIVE, "bit");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_FIELD, "bit");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_NUMERIC, "int");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_TRUE_FALSE, "char");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_YES_NO, "char");
		expectedDBTypeForEntityType.put( DataType.INT_PRIMATIVE, "int");
		expectedDBTypeForEntityType.put( DataType.INTEGER_FIELD, "int");
		expectedDBTypeForEntityType.put( DataType.LONG_PRIMATIVE, "bigint");
		expectedDBTypeForEntityType.put( DataType.LONG_FIELD, "bigint");
		expectedDBTypeForEntityType.put( DataType.FLOAT_PRIMATIVE, "float");
		expectedDBTypeForEntityType.put( DataType.FLOAT_FIELD, "float");
		expectedDBTypeForEntityType.put( DataType.DOUBLE_PRIMATIVE, "float");
		expectedDBTypeForEntityType.put( DataType.DOUBLE_FIELD, "float");
		expectedDBTypeForEntityType.put( DataType.BYTE_PRIMATIVE, "smallint");
		expectedDBTypeForEntityType.put( DataType.BYTE_FIELD, "smallint");
		expectedDBTypeForEntityType.put( DataType.BYTES_PRIMATIVE, "varbinary");
		expectedDBTypeForEntityType.put( DataType.URL, "varchar");
		expectedDBTypeForEntityType.put( DataType.TIMEZONE, "varchar");
		expectedDBTypeForEntityType.put( DataType.DATE_TEMPORAL_TYPE, "date");
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIMESTAMP_TEMPORAL_TYPE, "datetime2");
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIME_TEMPORAL_TYPE, "time");
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_DATE_TEMPORAL_TYPE, "date");
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_TIMESTAMP_TEMPORAL_TYPE, "datetime2");
		expectedDBTypeForEntityType.put( DataType.LOCALDATE, "date");
		expectedDBTypeForEntityType.put( DataType.LOCALTIME, "time");
		expectedDBTypeForEntityType.put( DataType.LOCALDATETIME, "datetime2");
		expectedDBTypeForEntityType.put( DataType.BIGINTEGER, "numeric");
		expectedDBTypeForEntityType.put( DataType.BIGDECIMAL, "numeric");
		expectedDBTypeForEntityType.put( DataType.SERIALIZABLE, "varbinary");
		expectedDBTypeForEntityType.put( DataType.UUID, "binary");
		expectedDBTypeForEntityType.put( DataType.INSTANT, "datetime2");
		expectedDBTypeForEntityType.put( DataType.DURATION, "bigint");
		expectedDBTypeForEntityType.put( DataType.CHARACTER, "char");
		expectedDBTypeForEntityType.put( DataType.TEXT, "text");
		expectedDBTypeForEntityType.put( DataType.STRING, "varchar");
	}}

	/**
	 * Holds configuration for the Microsoft SQL Server database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final MSSQLServerContainer<?> mssqlserver = new MSSQLServerContainer<>( IMAGE_NAME )
			.acceptLicense()
			.withPassword( PASSWORD )
			.withReuse( true );

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	private String getRegularJdbcUrl() {
		return "jdbc:sqlserver://localhost:1433";
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	@Override
	public String getDatatypeQuery(String tableName, String columnName) {
		return findTypeForColumnBaseQuery.replace(
				TABLE_PARAM, tableName.toLowerCase() ).replace(
				COLUMN_PARAM, columnName.toLowerCase() );
	}

	@Override
	public String getExpectedDatatype(DataType dataType) {
		return expectedDBTypeForEntityType.get(dataType);
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			mssqlserver.start();
			return mssqlserver.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	private String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ";user=" + mssqlserver.getUsername() + ";password=" + mssqlserver.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "sqlserver://" + mssqlserver.getUsername() + ":" + mssqlserver.getPassword() + "@" + jdbcUrl.substring( "jdbc:sqlserver://".length() );
	}

	private MSSQLServerDatabase() {
	}

}

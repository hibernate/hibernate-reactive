/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.util.EnumMap;
import java.util.Map;

import org.testcontainers.containers.Db2Container;

class DB2Database implements TestableDatabase {

	public static DB2Database INSTANCE = new DB2Database();

	public final static String IMAGE_NAME = "ibmcom/db2:11.5.5.1";

	String findTypeForColumnBaseQuery =
			"SELECT TYPENAME FROM SYSCAT.COLUMNS where TABNAME = '" + TABLE_PARAM + "' and COLNAME = '" + COLUMN_PARAM + "'";

	public static Map<DataType, String> expectedDBTypeForEntityType = new EnumMap<DataType, String>( DataType.class);
	static {{
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_PRIMATIVE, "SMALLINT");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_FIELD, "SMALLINT");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_NUMERIC, "INTEGER");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_TRUE_FALSE, "CHARACTER");
		expectedDBTypeForEntityType.put( DataType.BOOLEAN_YES_NO, "CHARACTER");
		expectedDBTypeForEntityType.put( DataType.INT_PRIMATIVE, "INTEGER");
		expectedDBTypeForEntityType.put( DataType.INTEGER_FIELD, "INTEGER");
		expectedDBTypeForEntityType.put( DataType.LONG_PRIMATIVE, "BIGINT");
		expectedDBTypeForEntityType.put( DataType.LONG_FIELD, "BIGINT");
		expectedDBTypeForEntityType.put( DataType.FLOAT_PRIMATIVE, "DOUBLE");
		expectedDBTypeForEntityType.put( DataType.FLOAT_FIELD, "DOUBLE");
		expectedDBTypeForEntityType.put( DataType.DOUBLE_PRIMATIVE, "DOUBLE");
		expectedDBTypeForEntityType.put( DataType.DOUBLE_FIELD, "DOUBLE");
		expectedDBTypeForEntityType.put( DataType.BYTE_PRIMATIVE, "SMALLINT");
		expectedDBTypeForEntityType.put( DataType.BYTE_FIELD, "SMALLINT");
		expectedDBTypeForEntityType.put( DataType.BYTES_PRIMATIVE, "VARCHAR");
		expectedDBTypeForEntityType.put( DataType.URL, "VARCHAR");
		expectedDBTypeForEntityType.put( DataType.TIMEZONE, "VARCHAR");
		expectedDBTypeForEntityType.put( DataType.DATE_TEMPORAL_TYPE, "DATE");
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIMESTAMP_TEMPORAL_TYPE, "TIMESTAMP");
		expectedDBTypeForEntityType.put( DataType.DATE_AS_TIME_TEMPORAL_TYPE, "TIME");
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_DATE_TEMPORAL_TYPE, "DATE");
		expectedDBTypeForEntityType.put( DataType.CALENDAR_AS_TIMESTAMP_TEMPORAL_TYPE, "TIMESTAMP");
		expectedDBTypeForEntityType.put( DataType.LOCALDATE, "DATE");
		expectedDBTypeForEntityType.put( DataType.LOCALTIME, "TIME");
		expectedDBTypeForEntityType.put( DataType.LOCALDATETIME, "TIMESTAMP");
		expectedDBTypeForEntityType.put( DataType.BIGINTEGER, "DECIMAL");
		expectedDBTypeForEntityType.put( DataType.BIGDECIMAL, "DECIMAL");
		expectedDBTypeForEntityType.put( DataType.SERIALIZABLE, "VARCHAR");
		expectedDBTypeForEntityType.put( DataType.UUID, "VARCHAR");
		expectedDBTypeForEntityType.put( DataType.INSTANT, "TIMESTAMP");
		expectedDBTypeForEntityType.put( DataType.DURATION, "BIGINT");
		expectedDBTypeForEntityType.put( DataType.CHARACTER, "CHARACTER");
		expectedDBTypeForEntityType.put( DataType.TEXT, "VARCHAR");
		expectedDBTypeForEntityType.put( DataType.STRING, "VARCHAR");
	}}

	/**
	 * Holds configuration for the DB2 database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	static final Db2Container db2 = new Db2Container( IMAGE_NAME )
		      .withUsername(DatabaseConfiguration.USERNAME)
		      .withPassword(DatabaseConfiguration.PASSWORD)
		      .withDatabaseName(DatabaseConfiguration.DB_NAME)
		      .acceptLicense()
		      .withReuse(true);

	private String getRegularJdbcUrl() {
		return "jdbc:db2://localhost:50000/" + db2.getDatabaseName();
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
				TABLE_PARAM, tableName.toUpperCase() ).replace(
				COLUMN_PARAM, columnName.toUpperCase() );
	}

	@Override
	public String getExpectedDatatype(DataType dataType) {
		return expectedDBTypeForEntityType.get(dataType);
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			db2.start();
			return db2.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ":user=" + db2.getUsername() + ";password=" + db2.getPassword() + ";";
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "db2://" + db2.getUsername() + ":" + db2.getPassword() + "@" + jdbcUrl.substring(11);
	}

	private DB2Database() {
	}

}

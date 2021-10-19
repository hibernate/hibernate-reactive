/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

/**
 * A database that we use for testing.
 */
public interface TestableDatabase {
	enum DataType {
		BOOLEAN_PRIMATIVE, BOOLEAN_FIELD, BOOLEAN_TRUE_FALSE, BOOLEAN_YES_NO, BOOLEAN_NUMERIC,
		INT_PRIMATIVE, INTEGER_FIELD,
		LONG_PRIMATIVE, LONG_FIELD,
		FLOAT_PRIMATIVE, FLOAT_FIELD,
		DOUBLE_PRIMATIVE, DOUBLE_FIELD,
		BYTE_PRIMATIVE, BYTE_FIELD,
		BYTES_PRIMATIVE,
		URL,
		TIMEZONE,
		DATE_TEMPORAL_TYPE, DATE_AS_TIME_TEMPORAL_TYPE, DATE_AS_TIMESTAMP_TEMPORAL_TYPE,
		CALENDAR_AS_DATE_TEMPORAL_TYPE, CALENDAR_AS_TIMESTAMP_TEMPORAL_TYPE,
		LOCALDATE, LOCALTIME, LOCALDATETIME,
		BIGINTEGER, BIGDECIMAL,
		SERIALIZABLE,
		UUID,
		INSTANT,
		DURATION,
		CHARACTER, TEXT, STRING
	}

	String TABLE_PARAM = "$table";
	String COLUMN_PARAM = "$column";

	String getJdbcUrl();

	String getUri();

	String getDatatypeQuery(String tableName, String columnName);

	String getExpectedDatatype(DataType dataType);

}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.hibernate.type.NumericBooleanType;
import org.hibernate.type.TextType;
import org.hibernate.type.TrueFalseType;
import org.hibernate.type.YesNoType;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;

public class H2Database implements TestableDatabase {
	public static H2Database INSTANCE = new H2Database();

	private static Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {
		{
			expectedDBTypeForClass.put( boolean.class, "BOOLEAN" );
			expectedDBTypeForClass.put( Boolean.class, "BOOLEAN" );
			expectedDBTypeForClass.put( NumericBooleanType.class, "INTEGER" );
			expectedDBTypeForClass.put( TrueFalseType.class, "CHARACTER" );
			expectedDBTypeForClass.put( YesNoType.class, "CHARACTER" );
			expectedDBTypeForClass.put( int.class, "INTEGER" );
			expectedDBTypeForClass.put( Integer.class, "INTEGER" );
			expectedDBTypeForClass.put( long.class, "BIGINT" );
			expectedDBTypeForClass.put( Long.class, "BIGINT" );
			expectedDBTypeForClass.put( float.class, "DOUBLE PRECISION" );
			expectedDBTypeForClass.put( Float.class, "DOUBLE PRECISION" );
			expectedDBTypeForClass.put( double.class, "DOUBLE PRECISION" );
			expectedDBTypeForClass.put( Double.class, "DOUBLE PRECISION" );
			expectedDBTypeForClass.put( byte.class, "TINYINT" );
			expectedDBTypeForClass.put( Byte.class, "TINYINT" );
			expectedDBTypeForClass.put( PrimitiveByteArrayTypeDescriptor.class, "BINARY VARYING" );
			expectedDBTypeForClass.put( URL.class, "VARCHAR_IGNORECASE" );
			expectedDBTypeForClass.put( TimeZone.class, "VARCHAR_IGNORECASE" );
			expectedDBTypeForClass.put( Date.class, "DATE" );
			expectedDBTypeForClass.put( Timestamp.class, "TIMESTAMP" );
			expectedDBTypeForClass.put( Time.class, "TIME" );
			expectedDBTypeForClass.put( LocalDate.class, "DATE" );
			expectedDBTypeForClass.put( LocalTime.class, "time" );
			expectedDBTypeForClass.put( LocalDateTime.class, "TIMESTAMP" );
			expectedDBTypeForClass.put( BigInteger.class, "NUMERIC" );
			expectedDBTypeForClass.put( BigDecimal.class, "NUMERIC" );
			expectedDBTypeForClass.put( Serializable.class, "BINARY VARYING" );
			expectedDBTypeForClass.put( UUID.class, "binary" );
			expectedDBTypeForClass.put( Instant.class, "datetime" );
			expectedDBTypeForClass.put( Duration.class, "bigint" );
			expectedDBTypeForClass.put( Character.class, "VARCHAR_IGNORECASE" );
			expectedDBTypeForClass.put( char.class, "VARCHAR_IGNORECASE" );
			expectedDBTypeForClass.put( TextType.class, "text" );
			expectedDBTypeForClass.put( String.class, "VARCHAR_IGNORECASE" );
		}
	}

	private String getRegularJdbcUrl() {
		return "jdbc:h2:~/test;DATABASE_TO_UPPER=FALSE";
	}

	@Override
	public String getJdbcUrl() {
		return "jdbc:h2:~/test;DATABASE_TO_UPPER=FALSE";
	}

	@Override
	public String getUri() {
		return "h2:~/test;DATABASE_TO_UPPER=FALSE";
	}


	@Override
	public String getScheme() {
		return "h2:";
	}

	@Override
	public String getNativeDatatypeQuery(String tableName, String columnName) {
		return "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS COLS " +
				"WHERE COLS.TABLE_NAME = '" + tableName + "'" +
				"AND COLS.COLUMN_NAME= '" + columnName + "'";
	}

	@Override
	public String getExpectedNativeDatatype(Class<?> dataType) {
		return expectedDBTypeForClass.get( dataType );
	}

	@Override
	public String createJdbcUrl(String host, int port, String database, Map<String, String> params) {
		// Primary mode for H2 is embedded which uses the URL format:   "jdbc:h2:~/test"
		// H2 can also be configured as a remote server.
		//  	EXAMPLE 1:  jdbc:h2:tcp://localhost/D:/myproject/data/project-name
		//  	EXAMPLE 2:  jdbc:h2:tcp://localhost/~/test
		//  	EXAMpLE 3:  jdbc:h2:tcp://localhost:9081/~/test
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
		if ( paramsBuilder.length() > 0 ) {
			url += jdbcStartQuery() + paramsBuilder.substring( 1 );
		}
		if ( database != null ) {
			return url + ";database=" + database;
		}
		return url;
	}

	@Override
	public String jdbcStartQuery() {
		return ";";
	}

	@Override
	public String jdbcParamDelimiter() {
		return ";";
	}

	private H2Database() {
	}
}

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

import org.testcontainers.containers.OracleContainer;

/**
 * Connection string for Oracle thin should be something like:
 *
 * jdbc:oracle:thin:[<user>/<password>]@<host>[:<port>]:<SID>
 */
class OracleDatabase implements TestableDatabase {

	public static final String IMAGE_NAME = "gvenzl/oracle-xe:18-slim";

	public static final OracleDatabase INSTANCE = new OracleDatabase();

	public static Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {{
		expectedDBTypeForClass.put( boolean.class, "NUMBER" );
		expectedDBTypeForClass.put( Boolean.class, "NUMBER" );
		expectedDBTypeForClass.put( NumericBooleanType.class, "NUMBER" );
		expectedDBTypeForClass.put( TrueFalseType.class, "CHAR" );
		expectedDBTypeForClass.put( YesNoType.class, "CHAR" );
		expectedDBTypeForClass.put( int.class, "NUMBER" );
		expectedDBTypeForClass.put( Integer.class, "NUMBER" );
		expectedDBTypeForClass.put( long.class, "NUMBER" );
		expectedDBTypeForClass.put( Long.class, "NUMBER" );
		expectedDBTypeForClass.put( float.class, "FLOAT" );
		expectedDBTypeForClass.put( Float.class, "FLOAT" );
		expectedDBTypeForClass.put( double.class, "FLOAT" );
		expectedDBTypeForClass.put( Double.class, "FLOAT" );
		expectedDBTypeForClass.put( byte.class, "NUMBER" );
		expectedDBTypeForClass.put( Byte.class, "NUMBER" );
		expectedDBTypeForClass.put( PrimitiveByteArrayTypeDescriptor.class, "BLOB" );
		expectedDBTypeForClass.put( URL.class, "VARCHAR2" );
		expectedDBTypeForClass.put( TimeZone.class, "VARCHAR2" );
		expectedDBTypeForClass.put( Date.class, "DATE" );
		expectedDBTypeForClass.put( Timestamp.class, "TIMESTAMP(6)" );
		expectedDBTypeForClass.put( Time.class, "DATE" );
		expectedDBTypeForClass.put( LocalDate.class, "DATE" );
		expectedDBTypeForClass.put( LocalTime.class, "TIMESTAMP(6)" );
		expectedDBTypeForClass.put( LocalDateTime.class, "TIMESTAMP(6)" );
		expectedDBTypeForClass.put( BigInteger.class, "NUMBER" );
		expectedDBTypeForClass.put( BigDecimal.class, "NUMBER" );
		expectedDBTypeForClass.put( Serializable.class, "RAW" );
		expectedDBTypeForClass.put( UUID.class, "RAW" );
		expectedDBTypeForClass.put( Instant.class, "TIMESTAMP(6)" );
		expectedDBTypeForClass.put( Duration.class, "NUMBER" );
		expectedDBTypeForClass.put( Character.class, "CHAR" );
		expectedDBTypeForClass.put( char.class, "CHAR" );
		expectedDBTypeForClass.put( TextType.class, "VARCHAR2" );
		expectedDBTypeForClass.put( String.class, "VARCHAR2" );
	}};

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
	public String getNativeDatatypeQuery(String tableName, String columnName) {
		return "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' AND COLUMN_NAME = '" + columnName.toUpperCase() + "'";
	}

	@Override
	public String getExpectedNativeDatatype(Class<?> dataType) {
		return expectedDBTypeForClass.get( dataType );
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

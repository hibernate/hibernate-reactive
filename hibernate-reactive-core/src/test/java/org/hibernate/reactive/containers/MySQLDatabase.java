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

import static org.hibernate.reactive.containers.DockerImage.imageName;

import org.testcontainers.containers.MySQLContainer;

class MySQLDatabase implements TestableDatabase {

	static MySQLDatabase INSTANCE = new MySQLDatabase();

	private static Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {{
		expectedDBTypeForClass.put( boolean.class, "bit" );
		expectedDBTypeForClass.put( Boolean.class, "bit" );
		expectedDBTypeForClass.put( NumericBooleanType.class, "int" );
		expectedDBTypeForClass.put( TrueFalseType.class, "char" );
		expectedDBTypeForClass.put( YesNoType.class, "char" );
		expectedDBTypeForClass.put( int.class, "int" );
		expectedDBTypeForClass.put( Integer.class, "int" );
		expectedDBTypeForClass.put( long.class, "bigint" );
		expectedDBTypeForClass.put( Long.class, "bigint" );
		expectedDBTypeForClass.put( float.class, "float" );
		expectedDBTypeForClass.put( Float.class, "float" );
		expectedDBTypeForClass.put( double.class, "double" );
		expectedDBTypeForClass.put( Double.class, "double" );
		expectedDBTypeForClass.put( byte.class, "tinyint" );
		expectedDBTypeForClass.put( Byte.class, "tinyint" );
		expectedDBTypeForClass.put( PrimitiveByteArrayTypeDescriptor.class, "tinyblob" );
		expectedDBTypeForClass.put( URL.class, "varchar" );
		expectedDBTypeForClass.put( TimeZone.class, "varchar" );
		expectedDBTypeForClass.put( Date.class, "date" );
		expectedDBTypeForClass.put( Timestamp.class, "datetime" );
		expectedDBTypeForClass.put( Time.class, "time" );
		expectedDBTypeForClass.put( LocalDate.class, "date" );
		expectedDBTypeForClass.put( LocalTime.class, "time" );
		expectedDBTypeForClass.put( LocalDateTime.class, "datetime" );
		expectedDBTypeForClass.put( BigInteger.class, "decimal" );
		expectedDBTypeForClass.put( BigDecimal.class, "decimal" );
		expectedDBTypeForClass.put( Serializable.class, "tinyblob" );
		expectedDBTypeForClass.put( UUID.class, "binary" );
		expectedDBTypeForClass.put( Instant.class, "datetime" );
		expectedDBTypeForClass.put( Duration.class, "bigint" );
		expectedDBTypeForClass.put( Character.class, "char" );
		expectedDBTypeForClass.put( char.class, "char" );
		expectedDBTypeForClass.put( TextType.class, "text" );
		expectedDBTypeForClass.put( String.class, "varchar" );
	}};

	/**
	 * Holds configuration for the MySQL database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final MySQLContainer<?> mysql = new MySQLContainer<>( imageName( "mysql", "8.0.28") )
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
	public String getExpectedNativeDatatype(Class<?> dataType) {
		return expectedDBTypeForClass.get( dataType );
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

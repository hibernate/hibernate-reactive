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

import org.testcontainers.containers.PostgreSQLContainer;

class PostgreSQLDatabase implements TestableDatabase {

	public static PostgreSQLDatabase INSTANCE = new PostgreSQLDatabase();

	public final static String IMAGE_NAME = "postgres:14.1";

	private static final Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {{
			expectedDBTypeForClass.put( boolean.class, "boolean" );
			expectedDBTypeForClass.put( Boolean.class, "boolean" );
			expectedDBTypeForClass.put( NumericBooleanType.class, "integer" );
			expectedDBTypeForClass.put( TrueFalseType.class, "character" );
			expectedDBTypeForClass.put( YesNoType.class, "character" );
			expectedDBTypeForClass.put( int.class, "integer" );
			expectedDBTypeForClass.put( Integer.class, "integer" );
			expectedDBTypeForClass.put( long.class, "bigint" );
			expectedDBTypeForClass.put( Long.class, "bigint" );
			expectedDBTypeForClass.put( float.class, "real" );
			expectedDBTypeForClass.put( Float.class, "real" );
			expectedDBTypeForClass.put( double.class, "double precision" );
			expectedDBTypeForClass.put( Double.class, "double precision" );
			expectedDBTypeForClass.put( byte.class, "smallint" );
			expectedDBTypeForClass.put( Byte.class, "smallint" );
			expectedDBTypeForClass.put( PrimitiveByteArrayTypeDescriptor.class, "bytea" );
			expectedDBTypeForClass.put( URL.class, "character varying" );
			expectedDBTypeForClass.put( TimeZone.class, "character varying" );
			expectedDBTypeForClass.put( Date.class, "date" );
			expectedDBTypeForClass.put( Timestamp.class, "timestamp without time zone" );
			expectedDBTypeForClass.put( Time.class, "time without time zone" );
			expectedDBTypeForClass.put( LocalDate.class, "date" );
			expectedDBTypeForClass.put( LocalTime.class, "time without time zone" );
			expectedDBTypeForClass.put( LocalDateTime.class, "timestamp without time zone" );
			expectedDBTypeForClass.put( BigInteger.class, "numeric" );
			expectedDBTypeForClass.put( BigDecimal.class, "numeric" );
			expectedDBTypeForClass.put( Serializable.class, "bytea" );
			expectedDBTypeForClass.put( UUID.class, "binary" );
			expectedDBTypeForClass.put( Instant.class, "timestamp without time zone" );
			expectedDBTypeForClass.put( Duration.class, "bigint" );
			expectedDBTypeForClass.put( Character.class, "character" );
			expectedDBTypeForClass.put( char.class, "character" );
			expectedDBTypeForClass.put( TextType.class, "text" );
			expectedDBTypeForClass.put( String.class, "character varying" );
	}}

	/**
	 * Holds configuration for the PostgreSQL database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>( IMAGE_NAME )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	protected PostgreSQLDatabase() {
	}

	@Override
	public String getConnectionUri() {
		return connectionUri( postgresql );
	}

	@Override
	public String getDefaultUrl() {
		return "postgresql://" + TestableDatabase.credentials( postgresql ) + "localhost:5432/" + postgresql.getDatabaseName();
	}

	@Override
	public String getNativeDatatypeQuery(String tableName, String columnName) {
		return TestableDatabase.super.getNativeDatatypeQuery( tableName.toLowerCase(), columnName.toLowerCase() );
	}

	@Override
	public String getExpectedNativeDatatype(Class<?> dataType) {
		return expectedDBTypeForClass.get( dataType );
	}
}

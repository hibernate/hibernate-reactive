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
import org.testcontainers.utility.DockerImageName;

class PostgreSQLDatabase implements TestableDatabase {

	public static PostgreSQLDatabase INSTANCE = new PostgreSQLDatabase();

	public static final String IMAGE_NAME = "postgres";
	public static final String IMAGE_VERSION = ":14.2";

	private static Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

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
	public static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>(
			DockerImageName.parse( DOCKER_REPOSITORY + IMAGE_NAME + IMAGE_VERSION ).asCompatibleSubstituteFor( IMAGE_NAME ) )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	private String getRegularJdbcUrl() {
		return "jdbc:postgresql://localhost:5432/" + postgresql.getDatabaseName();
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
		return TestableDatabase.super.getNativeDatatypeQuery( tableName.toLowerCase(), columnName.toLowerCase() );
	}

	@Override
	public String getExpectedNativeDatatype(Class<?> dataType) {
		return expectedDBTypeForClass.get( dataType );
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			postgresql.start();
			// Latest Postgres JDBC driver has dropped support for loggerLevel
			// and the Vert.x driver throws an exception because it does not recognize it
			return postgresql.getJdbcUrl().replace( "?loggerLevel=OFF", "" );
		}

		return getRegularJdbcUrl();
	}

	private static String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + "?user=" + postgresql.getUsername() + "&password=" + postgresql.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "postgresql://" + postgresql.getUsername() + ":" + postgresql.getPassword() + "@" + jdbcUrl.substring(18);
	}

	protected PostgreSQLDatabase() {
	}
}

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

import static org.hibernate.reactive.containers.DockerImage.imageName;

/**
 * Connection string for Oracle thin should be something like:
 *
 * jdbc:oracle:thin:[<user>/<password>]@<host>[:<port>][/<databaseName>]
 */
class OracleDatabase implements TestableDatabase {

	public static final OracleDatabase INSTANCE = new OracleDatabase();

	public static Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {
		{
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
		}
	}

	public static final OracleContainer oracle = new OracleContainer( imageName( "gvenzl/oracle-xe", "21.3.0-slim" ) )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withLogConsumer( of -> System.out.println( of.getUtf8String() ) )
			.withReuse( true )

			// We need to limit the maximum amount of CPUs being used by the container;
			// otherwise the hardcoded memory configuration of the DB might not be enough to successfully boot it.
			// See https://github.com/gvenzl/oci-oracle-xe/issues/64
			// I choose to limit it to "2 cpus": should be more than enough for any local testing needs,
			// and keeps things simple.
			.withCreateContainerCmdModifier( cmd -> cmd.getHostConfig().withCpuCount( 2l ) );

	@Override
	public String getJdbcUrl() {
		return addCredentialsAsParameters( address() );
	}

	private String getRegularJdbcUrl() {
		return "jdbc:oracle:thin:" + getCredentials() + "@localhost:1521/" + oracle.getDatabaseName();
	}

	@Override
	public String createJdbcUrl(String host, int port, String database, Map<String, String> params) {
		final StringBuilder paramsBuilder = new StringBuilder();
		if ( params != null && !params.isEmpty() ) {
			params.forEach( (key, value) -> {
				paramsBuilder.append( jdbcParamDelimiter() );
				paramsBuilder.append( key );
				paramsBuilder.append( "=" );
				paramsBuilder.append( value );
			} );
		}

		String url = "jdbc:oracle:thin:@" + host;
		if ( port > -1 ) {
			url += ":" + port;
		}
		if ( database != null && !database.isBlank() ) {
			url += "/" + database;
		}
		if ( paramsBuilder.length() > 0 ) {
			url += jdbcStartQuery() + paramsBuilder.substring( 1 );
		}
		return url;
	}

	@Override
	public String getScheme() {
		return "oracle:thin:";
	}

	@Override
	public String getUri() {
		// The url is different here because we expect it to work with io.vertx.oracleclient.impl.OracleConnectionUriParser
		return addCredentialsToUri( address() );
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

	private static String getCredentials() {
		if ( oracle.getUsername() == null
				|| oracle.getUsername().trim().length() == 0 ) {
			return "";
		}
		return oracle.getUsername() + "/" + oracle.getPassword();
	}

	private static String addCredentialsAsParameters(String jdbcUrl) {
		return jdbcUrl + "?user=" + oracle.getUsername() + "&password=" + oracle.getPassword();
	}

	private static String addCredentialsToUri(String jdbcUrl) {
		// The JDBC url will look something like:
		// jdbc:oracle:thin:@localhost:49413/hreact
		String uri = jdbcUrl;
		if ( uri.startsWith( "jdbc:" ) ) {
			jdbcUrl.substring( "jdbc:".length() );
		}
		uri = uri.replace( "thin:@", "thin:" + getCredentials() + "@" );
		return uri;
	}

	private OracleDatabase() {
	}
}

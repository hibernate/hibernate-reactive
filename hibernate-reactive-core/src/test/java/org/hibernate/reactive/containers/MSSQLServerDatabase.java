/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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

import org.hibernate.type.NumericBooleanConverter;
import org.hibernate.type.TrueFalseConverter;
import org.hibernate.type.YesNoConverter;

import org.testcontainers.containers.MSSQLServerContainer;

import static org.hibernate.reactive.containers.DockerImage.fromDockerfile;

/**
 * The JDBC driver syntax is:
 *     jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]
 *
 * But the Vert.x SQL client syntax is:
 *     sqlserver://[user[:[password]]@]host[:port][/database][?attribute1=value1&attribute2=value2…​]
 */
class MSSQLServerDatabase implements TestableDatabase {

	public static final MSSQLServerDatabase INSTANCE = new MSSQLServerDatabase();

	public static final String PASSWORD = "~!HReact!~";

	public static Map<Class<?>, String> expectedDBTypeForClass = new HashMap<>();

	static {{
		expectedDBTypeForClass.put( boolean.class, "bit" );
		expectedDBTypeForClass.put( Boolean.class, "bit" );

		expectedDBTypeForClass.put( NumericBooleanConverter.class, "int" );
		expectedDBTypeForClass.put( YesNoConverter.class, "char" );
		expectedDBTypeForClass.put( TrueFalseConverter.class, "char" );
		expectedDBTypeForClass.put( byte[].class, "varbinary" );
		// expectedDBTypeForClass.put( TextType.class, "text" );

		expectedDBTypeForClass.put( int.class, "int" );
		expectedDBTypeForClass.put( Integer.class, "int" );
		expectedDBTypeForClass.put( long.class, "bigint" );
		expectedDBTypeForClass.put( Long.class, "bigint" );
		expectedDBTypeForClass.put( float.class, "real" );
		expectedDBTypeForClass.put( Float.class, "real" );
		expectedDBTypeForClass.put( double.class, "float" );
		expectedDBTypeForClass.put( Double.class, "float" );
		expectedDBTypeForClass.put( byte.class, "smallint" );
		expectedDBTypeForClass.put( Byte.class, "smallint" );
		expectedDBTypeForClass.put( URL.class, "varchar" );
		expectedDBTypeForClass.put( TimeZone.class, "varchar" );
		expectedDBTypeForClass.put( Date.class, "date" );
		expectedDBTypeForClass.put( Timestamp.class, "datetime2" );
		expectedDBTypeForClass.put( Time.class, "time" );
		expectedDBTypeForClass.put( LocalDate.class, "date" );
		expectedDBTypeForClass.put( LocalTime.class, "time" );
		expectedDBTypeForClass.put( LocalDateTime.class, "datetime2" );
		expectedDBTypeForClass.put( BigInteger.class, "numeric" );
		expectedDBTypeForClass.put( BigDecimal.class, "numeric" );
		expectedDBTypeForClass.put( Serializable.class, "varbinary" );
		expectedDBTypeForClass.put( UUID.class, "binary" );
		expectedDBTypeForClass.put( Instant.class, "datetimeoffset" );
		expectedDBTypeForClass.put( Duration.class, "bigint" );
		expectedDBTypeForClass.put( Character.class, "char" );
		expectedDBTypeForClass.put( char.class, "char" );
		expectedDBTypeForClass.put( String.class, "varchar" );
		expectedDBTypeForClass.put( String[].class, "xml" );
		expectedDBTypeForClass.put( Long[].class, "xml" );
		expectedDBTypeForClass.put( BigDecimal[].class, "xml" );
		expectedDBTypeForClass.put( BigInteger[].class, "xml" );
		expectedDBTypeForClass.put( Boolean[].class, "xml" );
	}}

	/**
	 * Holds configuration for the Microsoft SQL Server database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final MSSQLServerContainer<?> mssqlserver = new MSSQLServerContainer<>( fromDockerfile( "sqlserver" ) )
			.acceptLicense()
			.withPassword( PASSWORD )
			.withReuse( true );

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	private String getRegularJdbcUrl() {
		return "jdbc:sqlserver://localhost:1433;Encrypt=false";
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
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			mssqlserver.start();
			return mssqlserver.getJdbcUrl();
		}

		return getRegularJdbcUrl();
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
		String url = "jdbc:" + getScheme() + "//" + host;
		if ( port > -1 ) {
			url += ":" + port;
		}
		if ( !paramsBuilder.isEmpty() ) {
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

	private String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ";user=" + mssqlserver.getUsername() + ";password=" + mssqlserver.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "sqlserver://" + mssqlserver.getUsername() + ":" + mssqlserver.getPassword() + "@" + jdbcUrl.substring( "jdbc:sqlserver://".length() );
	}

	private MSSQLServerDatabase() {
	}

}

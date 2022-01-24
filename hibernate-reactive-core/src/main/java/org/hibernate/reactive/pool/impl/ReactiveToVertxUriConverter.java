/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

/**
 * <p>
 * Vertx JDBC URI parsers expect a PostgreSQL-like structure which is not necessarily
 * supported by all DB's. This utility class provides logic that will
 * check a URI and convert it, if necessary, to adhere to the parser REGEX.
 *  </p>
 *  <br>
 * <p>
 * the Vert.x SQL client syntax is:
 *     sqlserver://[user[:[password]]@]host[:port][/database][?attribute1=value1&attribute2=value2…​]
 * </p>
 * <br>
 * DB2 and SQL Server URI's may require replacement of separator characters in order to
 * correctly use the SqlConnectOptions.fromUri() and a specific DB driver and parser.
 */
public class ReactiveToVertxUriConverter {
	private static final Log log = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public static int DEFAULT_PORT_PG = 5432;
	public static int DEFAULT_PORT_MYSQL = 3306;
	public static int DEFAULT_PORT_MARIADB = 3306;
	public static int DEFAULT_PORT_MSSQL = 1433;
	public static int DEFAULT_PORT_COCKROACHDB = 26257;
	public static int DEFAULT_PORT_DB2 = 50000;

	static public URI convertUriToVertx(URI uri) {
		String scheme = uri.getScheme();
		String path = uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		if ( scheme.equals( "db2" ) && database.indexOf( ':' ) > 0 ) {
			return convertUriToVertxIfDb2( uri );
		} else if ( scheme.equals( "sqlserver" ) ) {
			return convertUriToVertxIfSqlServer( uri );
		} else if( scheme.equals( "cockroachdb" ) ) {
			return convertUriToVertxIfCockroachDB( uri );
		}

		return uri;
	}

	/**
	 * Replace the JDBC URI for Db2 to the one accepted by the Vert.x reactive client
	 */
	static public URI convertUriToVertxIfDb2(URI uri) {
		String scheme = uri.getScheme();
		String path = uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		if ( database.indexOf( ':' ) > 0 ) {
			// DB2 URLs are a bit odd and have the format:
			// jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
			database = database.substring( 0, database.indexOf( ':' ) );
		}
		else {
			return uri;
		}

		String host = uri.getHost();
		int port = uri.getPort();

		// port may be empty. Check and set default port if necessary based on scheme
		if ( port == -1 ) {
			port = defaultPort( scheme );;
		}

		//see if the credentials were specified via properties
		Map<String, String> extraProperties = new HashMap<>();

		if ( database != null && database.length() > 0 ) {
			extraProperties.put( "database", database );
		}
		//check the query for named parameters
		//in case it's a JDBC-style URL
		String[] params = {};
		// DB2 URLs are a bit odd and have the format:
		// jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
		if ( scheme.equals( "db2" ) ) {
			int queryIndex = uri.getPath().indexOf( ':' ) + 1;
			if ( queryIndex > 0 ) {
				params = uri.getPath().substring( queryIndex ).split( ";" );
			}
		}

		parseProperties( uri, params, extraProperties );

		// build new URI string like ==>>  db2://localhost:44444/hreact?user=hreact&password=hreact
		return rebuildURI( "db2", host, port, database, extraProperties );
	}

	/**
	 * Replace the JDBC URI for MS SQLServer to the one accepted by the Vert.x reactive client
	 */
	static public URI convertUriToVertxIfSqlServer(URI uri) {
		String propertySeparator = ";";
		// EXAMPLE URL
		//  sqlserver://localhost:52618;user=SA;password=~!HReact!~
		String scheme = uri.getScheme();
		String path = uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		if ( !scheme.equals( "sqlserver" ) ) {
			return uri;
		}

		String host = uri.getHost();
		int port = uri.getPort();
		int index = uri.toString().indexOf( ';' );

		if ( index > 0 ) {
			// SQL Server separates parameters in the url with a semicolon (';')
			// and the URI class doesn't get the right value for host and port when the url
			// contains parameters
			URI uriWithoutParams = URI.create( uri.toString().substring( 0, index ) );
			host = uriWithoutParams.getHost();
			port = uriWithoutParams.getPort();
		}

		if ( port == -1 ) {
			port = defaultPort( scheme );
		}

		//see if the credentials were specified via properties
		Map<String, String> extraProperties = new HashMap<>();

		//check the query for named parameters
		//in case it's a JDBC-style URL
		String[] params = {};
		// SQL Server separates parameters in the url with a semicolon (';')
		// Ex: jdbc:sqlserver://<server>:<port>;<database>=AdventureWorks;user=<user>;password=<password>
		// jdbc:sqlserver://localhost:33333:database=hreact;user=testuser;password=testpassword;prop1=value1
		String s = uri.toString();
		String query = uri.getQuery();
		// separator may be either a ';' or '&'
		if ( query != null ) {
			int queryIndex = s.indexOf( '?' ) + 1;
			if ( queryIndex > 0 ) {
				params = s.substring( queryIndex ).split( "&" );
			}
		}
		else {
			int queryIndex = s.indexOf( ';' ) + 1;
			if ( queryIndex > 0 ) {
				params = s.substring( queryIndex ).split( ";" );
			}
		}

		String databaseAsProperty = parseProperties( uri, params, extraProperties );

		// Database may be set as property, so check if NULL then set
		if ( ( database == null || database.length() == 0 ) && databaseAsProperty != null ) {
			database = databaseAsProperty;
		}
		// build new URI string like ===>>  sqlserver://localhost:44444/hreact?user=hreact&password=hreact
		return rebuildURI( "sqlserver", host, port, database, extraProperties );
	}

	/**
	 * Replace the JDBC URI for CockroachDB to the one accepted by the Vert.x reactive client
	 *
	 */
	static public URI convertUriToVertxIfCockroachDB(URI uri) {
		String scheme = uri.getScheme();
		String path = uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		String host = uri.getHost();
		int port = uri.getPort();

		// port may be empty. Check and set default port if necessary based on scheme
		if ( port == -1 ) {
			port = defaultPort( scheme );
		}

		// now set scheme() to postgres
		scheme = "postgres";

		if ( database.indexOf( ':' ) > 0 ) {
			database = database.substring( 0, database.indexOf( ':' ) );
		}

		String userInfo = uri.getUserInfo();
		String username = null;
		String password = null;
		if (userInfo!=null) {
			String[] bits = userInfo.split(":");
			username = bits[0];
			if (bits.length>1) {
				password = bits[1];
			}
		}

		String[] params = {};
		String s = uri.toString();
		String query = uri.getQuery();
		if ( query != null ) {
			int queryIndex = s.indexOf( '?' ) + 1;
			if ( queryIndex > 0 ) {
				params = s.substring( queryIndex ).split( "&" );
			}
		}
		//see if the credentials were specified via properties
		Map<String, String> extraProperties = new HashMap<>();

		String databaseAsProperty = parseProperties( uri, params, extraProperties );

		// Database may be set as property, so check if NULL then set
		if ( ( database == null || database.length() == 0 ) && databaseAsProperty != null ) {
			database = databaseAsProperty;
		}
		// build new URI string like ===>>  sqlserver://localhost:44444/hreact?user=hreact&password=hreact
		return rebuildURI( "postgres", host, port, database, extraProperties );
	}

	private static String parseProperties(URI uri, String[] params, Map<String, String> extraProperties) {
		String databaseAsProperty = null;
		//if not, look for URI-style user info first
		String userInfo = uri.getUserInfo();
		if ( userInfo != null ) {
			String[] bits = userInfo.split( ":" );
			extraProperties.put( "user", bits[0] );
			if ( bits.length > 1 ) {
				extraProperties.put( "password", bits[1] );
			}
		}

		for ( String param : params ) {
			String[] keyValue = param.split( "=" );
			if ( keyValue[0].equals( "database" ) ) {
				databaseAsProperty = keyValue[1];
			}
			else {
				checkAndAddProperty( uri, keyValue[0], keyValue[1], extraProperties );
			}
		}
		return databaseAsProperty;
	}

	private static void checkAndAddProperty(URI uri, String key, String value, Map<String, String> properties) {
		// Check for general property duplicating user or password since they may already be properties from userInfo
		if ( properties.containsKey( key ) ) {
			throw log.duplicateUriProperty(key, value, uri.toString() );
		}
		else {
			properties.put( key, value );
		}
	}

	private static URI rebuildURI(String dbName, String host, int port,
			String database, //String username, String password,
			Map<String, String> extraProperties) {
		StringBuilder sb = new StringBuilder( dbName + "://" );
		sb.append( host ).append( ":" ).append( port );

		if ( database != null && !database.isEmpty() ) {
			sb.append( "/" ).append( database );
		}
		String username = extraProperties.get( "user" );
		String password = extraProperties.get( "password" );
		if ( username != null || password != null || !extraProperties.isEmpty() ) {
			sb.append( "?" );
		}
		if ( username != null ) {
			sb.append( "user=" ).append( username );
			if ( password != null ) {
				sb.append( "&" );
			}
		}

		if ( password != null ) {
			sb.append( "password=" ).append( password );
		}
		for ( Object key : extraProperties.keySet() ) {
			if ( !key.equals( "user" ) && !key.equals( "password" ) ) {
				String value = extraProperties.get( key );
				sb.append( "&" ).append( (String) key ).append( "=" ).append( value );
			}
		}

		return URI.create( sb.toString() );
	}

	public static boolean uriContains(String value, String uri) {
		return uri.contains( value );
	}




	public static int defaultPort(String scheme) {
		switch ( scheme ) {
			case "postgresql":
			case "postgres":
				return DEFAULT_PORT_PG;
			case "mariadb":
			case "mysql":
				return DEFAULT_PORT_MYSQL;
			case "db2":
				return DEFAULT_PORT_DB2;
			case "cockroachdb":
				return DEFAULT_PORT_COCKROACHDB;
			case "sqlserver":
				return DEFAULT_PORT_MSSQL;
			default:
				throw new IllegalArgumentException( "Unknown default port for scheme: " + scheme );
		}
	}

}

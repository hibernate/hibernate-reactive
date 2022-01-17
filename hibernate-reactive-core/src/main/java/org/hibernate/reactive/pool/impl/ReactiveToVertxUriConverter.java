/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Vertx JDBC URI parsers expect a PostgreSQL-like structure which is not neccessarily
 * supported by all DB's. This utility class provides logic that will
 * check a URI and convert it, if necessary, to adhere to the parser REGEX.
 * <p>
 * DB2 and SQL Server URI's may require replacement of separator characters in order to
 * correctly use the SqlConnectOptions.fromUri() and a specific DB driver and parser.
 */
public class ReactiveToVertxUriConverter {

	static public URI convertUriToVertx(URI uri) {
		String scheme = uri.getScheme();
		String path = uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		if ( scheme.equals( "db2" ) && database.indexOf( ':' ) > 0 ) {
			return convertUriToVertxIfDb2( uri );
		}
		else if ( scheme.equals( "sqlserver" ) ) {
			return convertUriToVertxIfSqlServer( uri );
		}
		else if ( uri.toString().contains( "cockroachdb" ) ) {
			return URI.create( uri.toString().replaceAll( "^cockroachdb:", "postgres:" ) );
		}

		return uri;
	}

	/**
	 * This utility method provides URI conversion to replace  the URI : and ; separators for DB2 with vertx's ? and &.
	 * Vertx manages a common URL syntax rather than multiple DB syntax's for their SqlConnectOptions.fromUri()
	 * implementations.
	 *
	 * @param uri
	 *
	 * @return uri
	 */
	static public URI convertUriToVertxIfDb2(URI uri) {
		String scheme = uri.getScheme();
		String path = uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		if ( scheme.equals( "db2" ) && database.indexOf( ':' ) > 0 ) {
			// DB2 URLs are a bit odd and have the format:
			// jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
			database = database.substring( 0, database.indexOf( ':' ) );
		}
		else {
			return uri;
		}

		String host = uri.getHost();
		int port = uri.getPort();

		if ( port == -1 ) {
			port = 50000;
		}

		//see if the credentials were specified via properties
		String username = null;
		String password = null;
		Map<String, String> extraProperties = new HashMap<>();

		//if not, look for URI-style user info first
		String userInfo = uri.getUserInfo();
		if ( userInfo != null ) {
			String[] bits = userInfo.split( ":" );
			username = bits[0];
			if ( bits.length > 1 ) {
				password = bits[1];
			}
		}
		else {
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

			for ( String param : params ) {
				if ( param.startsWith( "user=" ) ) {
					username = param.substring( 5 );
				}
				else if ( param.startsWith( "pass=" ) ) {
					password = param.substring( 5 );
				}
				else if ( param.startsWith( "password=" ) ) {
					password = param.substring( 9 );
				}
				else if ( param.startsWith( "database=" ) ) {
					database = param.substring( 9 );
				}
				else {
					String[] keyValuePair = param.split( "=" );
					extraProperties.put( keyValuePair[0].trim(), keyValuePair[1].trim() );
				}
			}
		}

		// build new URI string like:
		// db2://localhost:44444/hreact?user=hreact&password=hreact
		return rebuildURI( "db2", host, port, database, username, password, extraProperties );
	}

	/**
	 * This method provides URI conversion to replace  the URI : and ; separators for MSSQL with vertx's ? and &.
	 * Vertx manages a common URL syntax rather than multiple DB syntax's for their SqlConnectOptions.fromUri()
	 * implementations.
	 *
	 * @param uri
	 *
	 * @return uri
	 */
	static public URI convertUriToVertxIfSqlServer(URI uri) {
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
			port = 1433;
		}

		//see if the credentials were specified via properties
		String username = null;
		String password = null;
		Map<String, String> extraProperties = new HashMap<>();

		//if not, look for URI-style user info first
		String userInfo = uri.getUserInfo();
		if ( userInfo != null ) {
			String[] bits = userInfo.split( ":" );
			username = bits[0];
			if ( bits.length > 1 ) {
				password = bits[1];
			}
		}
		else {
			//check the query for named parameters
			//in case it's a JDBC-style URL
			String[] params = {};
			// SQL Server separates parameters in the url with a semicolon (';')
			// Ex: jdbc:sqlserver://<server>:<port>;<database>=AdventureWorks;user=<user>;password=<password>
			String query = uri.getQuery();
			String rawQuery = uri.getRawQuery();
			String s = uri.toString();
			int queryIndex = s.indexOf( ';' ) + 1;
			if ( queryIndex > 0 ) {
				params = s.substring( queryIndex ).split( ";" );
			}

			for ( String param : params ) {
				if ( param.startsWith( "user=" ) ) {
					username = param.substring( 5 );
				}
				else if ( param.startsWith( "pass=" ) ) {
					password = param.substring( 5 );
				}
				else if ( param.startsWith( "password=" ) ) {
					password = param.substring( 9 );
				}
				else if ( param.startsWith( "database=" ) ) {
					database = param.substring( 9 );
				}
				else {
					String[] keyValuePair = param.split( "=" );
					extraProperties.put( keyValuePair[0].trim(), keyValuePair[1].trim() );
				}
			}
		}

		// build new URI string like:
		// sqlserver://localhost:44444/hreact?user=hreact&password=hreact
		return rebuildURI( "sqlserver", host, port, database, username, password, extraProperties );
	}

	private static URI rebuildURI(String dbName, String host, int port, String database, String username,
			String password, Map<String, String> extraProperties) {
		StringBuilder sb = new StringBuilder( dbName + "://" );
		sb.append( host ).append( ":" ).append( port );
		if ( database != null && !database.isEmpty() ) {
			sb.append( "/" ).append( database );
		}
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
			String value = extraProperties.get( key );
			sb.append( "&" ).append( (String) key ).append( "=" ).append( value );
		}

		return URI.create( sb.toString() );
	}
}

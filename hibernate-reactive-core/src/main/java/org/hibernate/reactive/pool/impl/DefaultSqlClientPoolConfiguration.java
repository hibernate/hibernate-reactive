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
import java.util.concurrent.TimeUnit;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.Configurable;

import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

import static org.hibernate.internal.util.config.ConfigurationHelper.getInt;
import static org.hibernate.internal.util.config.ConfigurationHelper.getInteger;
import static org.hibernate.internal.util.config.ConfigurationHelper.getString;

/**
 * The default {@link SqlClientPoolConfiguration} which configures the
 * {@link io.vertx.sqlclient.Pool} using the Hibernate ORM configuration
 * properties defined in {@link Settings}.
 * <p>
 * A custom implementation of {@code SqlClientPoolConfiguration} might
 * choose to extend this class in order to reuse its built-in
 * functionality.
 */
public class DefaultSqlClientPoolConfiguration implements SqlClientPoolConfiguration, Configurable {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private static final int DEFAULT_POOL_SIZE = 5;

	private int poolSize;
	private Integer maxWaitQueueSize;
	private Integer connectTimeout;
	private Integer idleTimeout;
	private Integer cacheMaxSize;
	private Integer sqlLimit;
	private Integer poolCleanerPeriod;
	private String user;
	private String pass;

	@Override
	public void configure(Map configuration) {
		user = getString( Settings.USER, configuration );
		pass = getString( Settings.PASS, configuration );
		poolSize = getInt( Settings.POOL_SIZE, configuration, DEFAULT_POOL_SIZE );
		maxWaitQueueSize = getInteger( Settings.POOL_MAX_WAIT_QUEUE_SIZE, configuration );
		cacheMaxSize = getInteger( Settings.PREPARED_STATEMENT_CACHE_MAX_SIZE, configuration );
		sqlLimit = getInteger( Settings.PREPARED_STATEMENT_CACHE_SQL_LIMIT, configuration );
		connectTimeout = getInteger( Settings.POOL_CONNECT_TIMEOUT, configuration );
		idleTimeout = getInteger( Settings.POOL_IDLE_TIMEOUT, configuration );
		poolCleanerPeriod = getInteger( Settings.POOL_CLEANER_PERIOD, configuration );
	}

	@Override
	public PoolOptions poolOptions() {
		PoolOptions poolOptions = new PoolOptions();

		LOG.connectionPoolSize( poolSize );
		poolOptions.setMaxSize( poolSize );
		if ( maxWaitQueueSize != null ) {
			LOG.connectionPoolMaxWaitSize( maxWaitQueueSize );
			poolOptions.setMaxWaitQueueSize( maxWaitQueueSize );
		}
		if ( idleTimeout != null ) {
			LOG.connectionPoolIdleTimeout( idleTimeout );
			poolOptions.setIdleTimeout( idleTimeout );
			poolOptions.setIdleTimeoutUnit( TimeUnit.MILLISECONDS );
		}
		if ( connectTimeout != null ) {
			LOG.connectionPoolTimeout( connectTimeout );
			poolOptions.setConnectionTimeout( connectTimeout );
			poolOptions.setConnectionTimeoutUnit( TimeUnit.MILLISECONDS );
		}
		if ( poolCleanerPeriod != null ) {
			LOG.connectionPoolCleanerPeriod( poolCleanerPeriod );
			poolOptions.setPoolCleanerPeriod( poolCleanerPeriod );
		}
		return poolOptions;
	}

	@Override
	public SqlConnectOptions connectOptions(URI uri) {
		String scheme = uri.getScheme();
		String path = scheme.equals( "oracle" )
				? oraclePath( uri )
				: uri.getPath();

		String database = path.length() > 0
				? path.substring( 1 )
				: "";

		if ( scheme.equals( "db2" ) && database.indexOf( ':' ) > 0 ) {
			// DB2 URLs are a bit odd and have the format:
			// jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
			database = database.substring( 0, database.indexOf( ':' ) );
		}

		String host = findHost( uri, scheme );
		int port = findPort( uri, scheme );

		//see if the credentials were specified via properties
		String username = user;
		String password = pass;
		Map<String, String> extraProps = new HashMap<>();
		if ( username == null || password == null ) {
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
				else if ( scheme.contains( "sqlserver" ) ) {
					// SQL Server separates parameters in the url with a semicolon (';')
					// Ex: jdbc:sqlserver://<server>:<port>;<database>=AdventureWorks;user=<user>;password=<password>
					String query = uri.getQuery();
					String rawQuery = uri.getRawQuery();
					String s = uri.toString();
					int queryIndex = s.indexOf( ';' ) + 1;
					if ( queryIndex > 0 ) {
						params = s.substring( queryIndex ).split( ";" );
					}
				}
				else {
					final String query = scheme.equals( "oracle" )
							? oracleQuery( uri )
							: uri.getQuery();
					if ( query != null ) {
						params = query.split( "&" );
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
						final int position = param.indexOf( "=" );
						if ( position != -1 ) {
							// We assume the first '=' is the one separating key and value
							String key = param.substring( 0, position );
							String value = param.substring( position + 1 );
							extraProps.put( key, value );
						}
						else {
							// A key without a value
							extraProps.put( param, null );
						}
					}
				}
			}
		}

		if ( username == null ) {
			throw LOG.databaseUsernameNotSpecified();
		}

		SqlConnectOptions connectOptions = new SqlConnectOptions()
				.setHost( host )
				.setPort( port )
				.setDatabase( database )
				.setUser( username );

		if ( password != null ) {
			connectOptions.setPassword( password );
		}

		for ( String key : extraProps.keySet() ) {
			connectOptions.addProperty( key, extraProps.get( key ) );
		}

		//enable the prepared statement cache by default
		connectOptions.setCachePreparedStatements( true );

		if ( cacheMaxSize != null ) {
			if ( cacheMaxSize <= 0 ) {
				LOG.preparedStatementCacheDisabled();
				connectOptions.setCachePreparedStatements( false );
			}
			else {
				LOG.preparedStatementCacheMaxSize( cacheMaxSize );
				connectOptions.setCachePreparedStatements( true );
				connectOptions.setPreparedStatementCacheMaxSize( cacheMaxSize );
			}
		}

		if ( sqlLimit != null ) {
			LOG.preparedStatementCacheSQLLimit( sqlLimit );
			connectOptions.setPreparedStatementCacheSqlLimit( sqlLimit );
		}

		return connectOptions;
	}

	private int oraclePort(URI uri) {
		String s = uri.toString().substring( "oracle:thin:".length() );
		if ( s.indexOf( ':' ) > -1 ) {
			// Example: localhost:1234...
			s = s.substring( s.indexOf( ':' ) + 1 );
			if ( s.indexOf( '/' ) != -1 ) {
				// Example: 1234/
				s = s.substring( 0, s.indexOf( '/' ) );
				return Integer.valueOf( s );
			}
			if ( s.indexOf( '?' ) != -1 ) {
				// Example: 1234?param=value
				s = s.substring( 0, s.indexOf( '?' ) );
				return Integer.valueOf( s );
			}
			// Example: 1234
			return Integer.valueOf( s );
		}
		return -1;
	}

	// For Oracle the host part starts with a '@'
	// Example oracle:thin:[username/password]@localhost:1234/database?param=value
	private String oracleHost(URI uri) {
		String s = uri.toString();
		String host = s.substring( s.indexOf( '@' ) + 1 );
		int end = host.indexOf( ':' );
		if ( end == -1 ) {
			end = host.indexOf( '/' );
			if ( end == -1 ) {
				end = host.indexOf( '?' );
			}
		}
		return host.substring( 0, end );
	}

	private String oracleQuery(URI uri) {
		String string = uri.toString();
		int start = string.indexOf( '?' );
		return start == -1
				? null
				: string.substring( start + 1 );
	}

	private String oraclePath(URI uri) {
		String string = uri.toString();
		// Remove everything before localhost:port
		final int i = string.indexOf( '@' );
		string = string.substring( i + 1 );
		// Check the start of the path
		int start = string.indexOf( '/' );
		if ( start == -1 ) {
			return "";
		}
		int end = string.indexOf( '?' ) == -1
				? string.length()
				: string.indexOf( '?' );
		return string.substring( start, end );
	}

	// Example sqlserver://localhost:1433;database=name;user=somename
	private String sqlServerHost(URI uri) {
		String s = uri.toString();
		String remainder = s.substring( s.indexOf( "://" ) + 3 );
		char endOfHostPortChar = ';';
		String hostPortString = remainder.substring( 0, remainder.indexOf( endOfHostPortChar ) );
		int endOfHost = hostPortString.indexOf( ':' );
		if ( endOfHost == -1 ) {
			return hostPortString;
		}
		return hostPortString.substring( 0, endOfHost );
	}

	private int sqlServerPort(URI uri) {
		String s = uri.toString();
		String remainder = s.substring( s.indexOf( "://" ) + 3 );
		char endOfHostPortChar = ';';
		String hostPortString = remainder.substring( 0, remainder.indexOf( endOfHostPortChar ) );
		int startOfPort = hostPortString.indexOf( ':' );
		if ( startOfPort == -1 ) {
			return -1;
		}
		return Integer.valueOf( hostPortString.substring( startOfPort + 1 ) );
	}

	private String extractHost(URI uri) {
		String s = uri.toString();
		String remainder = s.substring( s.indexOf( "://" ) + 3 );
		char endOfHostPortChar = '/';
		String hostPortString = remainder.substring( 0, remainder.indexOf( endOfHostPortChar ) );
		int endOfHost = hostPortString.indexOf( ':' );
		if ( endOfHost == -1 ) {
			return hostPortString;
		}
		return hostPortString.substring( 0, endOfHost );
	}

	private int extractPort(URI uri) {
		String s = uri.toString();
		String remainder = s.substring( s.indexOf( "://" ) + 3 );
		char endOfHostPortChar = '/';
		String hostPortString = remainder.substring( 0, remainder.indexOf( endOfHostPortChar ) );
		int startOfPort = hostPortString.indexOf( ':' );
		if ( startOfPort == -1 ) {
			return -1;
		}
		return Integer.valueOf( hostPortString.substring( startOfPort + 1 ) );
	}

	private String findHost(URI uri, String scheme) {
		if ( uri.getHost() != null ) {
			return uri.getHost();
		}
		if ( "oracle".equals( scheme ) ) {
			return oracleHost( uri );
		}
		if ( "sqlserver".equals( scheme ) ) {
			// SqlServer host
			return sqlServerHost( uri );
		}
		return extractHost( uri );
	}

	private int findPort(URI uri, String scheme) {
		int port = -1;
		if ( "oracle".equals( scheme ) ) {
			port = oraclePort( uri );
		}
		else if ( "sqlserver".equals( scheme ) ) {
			port = sqlServerPort( uri );
		}
		else {
			port = extractPort( uri );
		}
		if ( port == -1 ) {
			return defaultPort( scheme );
		}
		return port;
	}

	private int defaultPort(String scheme) {
		switch ( scheme ) {
			case "postgresql":
			case "postgres":
				return 5432;
			case "mariadb":
			case "mysql":
				return 3306;
			case "db2":
				return 50000;
			case "cockroachdb":
				return 26257;
			case "sqlserver":
				return 1433;
			case "oracle":
				return 1521;
			default:
				throw new IllegalArgumentException( "Unknown default port for scheme: " + scheme );
		}
	}

}

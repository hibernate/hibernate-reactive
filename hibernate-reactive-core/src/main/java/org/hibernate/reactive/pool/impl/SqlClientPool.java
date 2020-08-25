/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.spi.Driver;
import org.hibernate.dialect.PostgreSQL9Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import java.net.URI;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 * The {@code Pool} itself is backed by an instance of {@link Vertx}
 * obtained via the {@link VertxInstance} service.
 */
public class SqlClientPool implements ReactiveConnectionPool, ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	private static final int DEFAULT_POOL_SIZE = 5;

	private Pool pool;
	private boolean showSQL;
	private boolean formatSQL;
	private ServiceRegistryImplementor serviceRegistry;
	private Map<?,?> configurationValues;
	private boolean usePostgresStyleParameters;

	public SqlClientPool() {}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void configure(Map configurationValues) {
		//TODO: actually extract the configuration values we need rather than keeping a reference to the whole map.
		this.configurationValues = configurationValues;

		showSQL = ConfigurationHelper.getBoolean( Settings.SHOW_SQL, configurationValues, false );
		formatSQL = ConfigurationHelper.getBoolean( Settings.FORMAT_SQL, configurationValues, false );
		usePostgresStyleParameters =
				serviceRegistry.getService(JdbcEnvironment.class).getDialect() instanceof PostgreSQL9Dialect;
	}

	@Override
	public void start() {
		if ( pool == null ) {
			pool = createPool(configurationValues);
		}
	}

	protected Pool createPool(Map<?,?> configurationValues) {
		Vertx vertx = serviceRegistry.getService( VertxInstance.class ).getVertx();
		return configurePool( configurationValues, vertx );
	}

	protected Pool configurePool(Map<?,?> configurationValues, Vertx vertx) {
		URI uri = jdbcUrl(configurationValues);
		SqlConnectOptions connectOptions = sqlConnectOptions( uri );
		PoolOptions poolOptions = poolOptions( configurationValues );

		try {
			// First try to load the Pool using the standard ServiceLoader pattern
			// This only works if exactly 1 Driver is on the classpath.
			return Pool.pool( vertx, connectOptions, poolOptions );
		}
		catch (ServiceConfigurationError e) {
			// Backup option if multiple drivers are on the classpath.
			// We will be able to remove this once Vertx 3.9.2 is available
			return findDriver( uri, e ).createPool( vertx, connectOptions, poolOptions );
		}
	}

	private URI jdbcUrl(Map<?,?> configurationValues) {
		final String url = ConfigurationHelper.getString( Settings.URL, configurationValues );
		CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000011: SQL Client URL [%s]", url );
		return parse( url );
	}

	private PoolOptions poolOptions(Map<?,?> configurationValues) {
		PoolOptions poolOptions = new PoolOptions();

		final int poolSize = ConfigurationHelper.getInt( Settings.POOL_SIZE, configurationValues, DEFAULT_POOL_SIZE );
		CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000012: Connection pool size: %d", poolSize );
		poolOptions.setMaxSize( poolSize );

		final Integer maxWaitQueueSize = ConfigurationHelper.getInteger( Settings.MAX_WAIT_QUEUE_SIZE, configurationValues );
		if (maxWaitQueueSize!=null) {
			CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000013: Connection pool max wait queue size: %d", maxWaitQueueSize );
			poolOptions.setMaxWaitQueueSize(maxWaitQueueSize);
		}

		return poolOptions;
	}

	private SqlConnectOptions sqlConnectOptions(URI uri) {

		String scheme = uri.getScheme();

		String database = uri.getPath().substring( 1 );
		if ( scheme.equals("db2") && database.indexOf( ':' ) > 0 ) {
			database = database.substring( 0, database.indexOf( ':' ) );
		}

		// FIXME: Check which values can be null
		String username = ConfigurationHelper.getString( Settings.USER, configurationValues );
		String password = ConfigurationHelper.getString( Settings.PASS, configurationValues );
		if (username==null || password==null) {
			String[] params = {};
			// DB2 URLs are a bit odd and have the format: jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
			if ( scheme.equals("db2") ) {
				int queryIndex = uri.getPath().indexOf(':') + 1;
				if (queryIndex > 0) {
					params = uri.getPath().substring(queryIndex).split(";");
				}
			} else {
				params = uri.getQuery().split("&");
			}
			for (String param : params) {
				if ( param.startsWith("user=") ) {
					username = param.substring(5);
				}
				if ( param.startsWith("pass=") ) {
					password = param.substring(5);
				}
				if ( param.startsWith("password=") ) {
					password = param.substring(9);
				}
			}
		}

		int port = uri.getPort();
		if (port==-1) {
			switch (scheme) {
				case "postgresql": port = 5432; break;
				case "mysql": port = 3306; break;
				case "db2": port = 50000; break;
			}
		}

		SqlConnectOptions connectOptions = new SqlConnectOptions()
				.setHost( uri.getHost() )
				.setPort( port )
				.setDatabase( database )
				.setUser( username );
		if (password != null) {
			connectOptions.setPassword( password );
		}

		//enable the prepared statement cache by default (except for DB2) and MySQL
		connectOptions.setCachePreparedStatements( !scheme.equals( "db2" ) && !scheme.equals( "mysql" ) );

		final Integer cacheMaxSize = ConfigurationHelper.getInteger( Settings.PREPARED_STATEMENT_CACHE_MAX_SIZE, configurationValues );
		if (cacheMaxSize!=null) {
			if (cacheMaxSize <= 0) {
				CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000014: Prepared statement cache disabled", cacheMaxSize );
				connectOptions.setCachePreparedStatements(false);
			}
			else {
				CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000015: Prepared statement cache max size: %d", cacheMaxSize );
				connectOptions.setCachePreparedStatements(true);
				connectOptions.setPreparedStatementCacheMaxSize(cacheMaxSize);
			}
		}

		final Integer sqlLimit = ConfigurationHelper.getInteger( Settings.PREPARED_STATEMENT_CACHE_SQL_LIMIT, configurationValues );
		if (sqlLimit!=null) {
			CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000016: Prepared statement cache SQL limit: %d", sqlLimit );
			connectOptions.setPreparedStatementCacheSqlLimit(sqlLimit);
		}

		return connectOptions;
	}

	private Driver findDriver(URI uri, ServiceConfigurationError originalError) {
		String scheme = uri.getScheme(); // "postgresql", "mysql", "db2", etc
		for (Driver d : ServiceLoader.load( Driver.class )) {
			String driverName = d.getClass().getCanonicalName();
			CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000013: Detected driver [%s]", driverName );
			if ("io.vertx.db2client.spi.DB2Driver".equals( driverName ) && "db2".equalsIgnoreCase( scheme )) {
				return  d;
			}
			if ("io.vertx.mysqlclient.spi.MySQLDriver".equals( driverName ) && "mysql".equalsIgnoreCase( scheme )) {
				return d;
			}
			if ("io.vertx.pgclient.spi.PgDriver".equals( driverName ) &&
					("postgre".equalsIgnoreCase( scheme ) ||
					 "postgres".equalsIgnoreCase( scheme ) ||
					 "postgresql".equalsIgnoreCase( scheme ))) {
				return d;
			}
		}
		throw new ConfigurationException( "No suitable drivers found for URI scheme: " + scheme, originalError );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection() {
		return Handlers.toCompletionStage(
				handler -> pool.getConnection(
						ar -> handler.handle(
								ar.succeeded()
										? succeededFuture( newConnection( ar.result() ) )
										: failedFuture( ar.cause() )
						)
				)
		);
	}

	private SqlClientConnection newConnection(SqlConnection ar) {
		return new SqlClientConnection( ar, showSQL, formatSQL, usePostgresStyleParameters );
	}

	@Override
	public ReactiveConnection getProxyConnection() {
		return new ProxyConnection();
	}

	@Override
	public void stop() {
		if ( pool != null ) {
			this.pool.close();
		}
	}

	public static URI parse(String url) {
		if ( url == null ) {
			return null;
		}

		if ( url.startsWith( "jdbc:" ) ) {
			return URI.create( url.substring( 5 ) );
		}

		return URI.create( url );
	}

	private class ProxyConnection implements ReactiveConnection {
		private ReactiveConnection connection;
		private boolean connected;

		private <T> CompletionStage<T> withConnection(Function<ReactiveConnection,CompletionStage<T>> operation) {
			if (!connected) {
				connected = true; // we're not allowed to fetch two connections!
				return getConnection()
						.thenApply(newConnection -> connection = newConnection)
						.thenCompose(operation);
			}
			else {
				if (connection == null) {
					// we're already in the process of fetching a connection,
					// so this must be an illegal concurrent call
					throw new IllegalStateException("session is currently connecting to database");
				}
				return operation.apply(connection);
			}
		}

		@Override
		public CompletionStage<Void> execute(String sql) {
			return withConnection( conn -> conn.execute(sql) );
		}

		@Override
		public CompletionStage<Integer> update(String sql) {
			return withConnection( conn -> conn.update(sql) );
		}

		@Override
		public CompletionStage<Integer> update(String sql, Object[] paramValues) {
			return withConnection( conn -> conn.update(sql, paramValues) );
		}

		@Override
		public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
			return withConnection( conn -> conn.update(sql, paramValues, false, expectation) );
		}

		@Override
		public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
			return withConnection( conn -> conn.update(sql, paramValues) );
		}

		@Override
		public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
			return withConnection( conn -> conn.updateReturning(sql, paramValues) );
		}

		@Override
		public CompletionStage<Result> select(String sql) {
			return withConnection( conn -> conn.select(sql) );
		}

		@Override
		public CompletionStage<Result> select(String sql, Object[] paramValues) {
			return withConnection( conn -> conn.select(sql, paramValues) );
		}

		@Override
		public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
			return withConnection( conn -> conn.selectJdbc(sql, paramValues) );
		}

		@Override
		public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
			return withConnection( conn -> conn.selectLong(sql, paramValues) );
		}

		@Override
		public CompletionStage<Void> beginTransaction() {
			return withConnection(ReactiveConnection::beginTransaction);
		}

		@Override
		public CompletionStage<Void> commitTransaction() {
			return withConnection(ReactiveConnection::commitTransaction);
		}

		@Override
		public CompletionStage<Void> rollbackTransaction() {
			return withConnection(ReactiveConnection::rollbackTransaction);
		}

		@Override
		public CompletionStage<Void> executeBatch() {
			return withConnection(ReactiveConnection::executeBatch);
		}

		@Override
		public void close() {
			if (connection!=null) {
				connection.close();
				connection = null;
			}
		}
	}
}

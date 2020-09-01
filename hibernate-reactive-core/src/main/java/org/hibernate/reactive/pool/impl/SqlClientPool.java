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
import static org.hibernate.internal.CoreLogging.messageLogger;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 * The {@code Pool} itself is backed by an instance of {@link Vertx}
 * obtained via the {@link VertxInstance} service.
 * <p>
 * This class may be extended by programs which wish to implement
 * custom connection management or multitenancy.
 *
 * @see SqlClientPoolConfiguration
 */
public class SqlClientPool implements ReactiveConnectionPool, ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	private Pool pool;
	private boolean showSQL;
	private boolean formatSQL;
	private URI uri;
	private ServiceRegistryImplementor serviceRegistry;
	private boolean usePostgresStyleParameters;

	public SqlClientPool() {}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void configure(Map configuration) {
		uri = jdbcUrl( configuration );
		showSQL = ConfigurationHelper.getBoolean( Settings.SHOW_SQL, configuration, false );
		formatSQL = ConfigurationHelper.getBoolean( Settings.FORMAT_SQL, configuration, false );
		usePostgresStyleParameters =
				serviceRegistry.getService(JdbcEnvironment.class).getDialect() instanceof PostgreSQL9Dialect;
	}

	@Override
	public void start() {
		if ( pool == null ) {
			pool = createPool( uri );
		}
	}

	public Pool getPool() {
		return pool;
	}

	/**
	 * Create a new {@link Pool} for the given JDBC URL or database URI,
	 * using the {@link VertxInstance} service to obtain an instance of
	 * {@link Vertx}, and the {@link SqlClientPoolConfiguration} service
	 * to obtain options for creating the connection pool.
	 *
	 * @param uri JDBC URL or database URI
	 *
	 * @return the new {@link Pool}
	 */
	protected Pool createPool(URI uri) {
		SqlClientPoolConfiguration configuration = serviceRegistry.getService(SqlClientPoolConfiguration.class);
		VertxInstance vertx = serviceRegistry.getService(VertxInstance.class);
		return createPool( uri, configuration.connectOptions( uri ), configuration.poolOptions(), vertx.getVertx() );
	}

	/**
	 * Create a new {@link Pool} for the given JDBC URL or database URI,
	 * connection pool options, and the given instance of {@link Vertx}.
	 *
	 * @param uri JDBC URL or database URI
	 * @param connectOptions the connection options
	 * @param poolOptions the connection pooling options
	 * @param vertx the instance of {@link Vertx} to be used by the pool
	 *
	 * @return the new {@link Pool}
	 */
	protected Pool createPool(URI uri, SqlConnectOptions connectOptions, PoolOptions poolOptions, Vertx vertx) {
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

	/**
	 * Determine the JDBC URL or database URI from the given configuration.
	 *
	 * @param configurationValues the configuration properties
	 *
	 * @return the JDBC URL as a {@link URI}
	 */
	protected URI jdbcUrl(Map<?,?> configurationValues) {
		String url = ConfigurationHelper.getString( Settings.URL, configurationValues );
		messageLogger(SqlClientPool.class).infof( "HRX000011: SQL Client URL [%s]", url );
		return parse( url );
	}

	/**
	 * When there are multiple candidate drivers in the classpath,
	 * {@link Pool#pool} throws a {@link ServiceConfigurationError},
	 * so we need to disambiguate according to the scheme specified
	 * in the given {@link URI}.
	 *
	 * @param uri the JDBC URL or database URI
	 * @param originalError the error that was thrown
	 *
	 * @return the disambiguated {@link Driver}
	 */
	private Driver findDriver(URI uri, ServiceConfigurationError originalError) {
		String scheme = uri.getScheme(); // "postgresql", "mysql", "db2", etc
		for (Driver d : ServiceLoader.load( Driver.class )) {
			String driverName = d.getClass().getCanonicalName();
			messageLogger(SqlClientPool.class).infof( "HRX000013: Detected driver [%s]", driverName );
			switch (driverName) {
				case "io.vertx.db2client.spi.DB2Driver":
					if ( "db2".equalsIgnoreCase( scheme ) ) {
						return d;
					}
				case "io.vertx.mysqlclient.spi.MySQLDriver":
					if ( "mysql".equalsIgnoreCase( scheme ) ) {
						return d;
					}
				case "io.vertx.pgclient.spi.PgDriver":
					if ( "postgre".equalsIgnoreCase( scheme ) ||
							"postgres".equalsIgnoreCase( scheme ) ||
							"postgresql".equalsIgnoreCase( scheme ) ) {
						return d;
					}
			}
		}
		throw new ConfigurationException( "No suitable drivers found for URI scheme: " + scheme, originalError );
	}

	/**
	 * Get a {@link Pool} for the specified tenant.
	 * <p>
	 * This is an unimplemented operation which must be overridden by
	 * subclasses which support multitenancy. For convenience, a
	 * subclass may create tenant pools by calling {@link #createPool(URI)}
	 * or {@link #createPool(URI, SqlConnectOptions, PoolOptions, Vertx)}.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @throws UnsupportedOperationException if multitenancy is not supported
	 *
	 * @see #getConnection(String)
	 */
	protected Pool getTenantPool(String tenantId) {
		throw new UnsupportedOperationException("multitenancy not supported by built-in SqlClientPool");
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection() {
		return getConnectionFromPool( pool );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection(String tenantId) {
		return getConnectionFromPool( getTenantPool( tenantId ) );
	}

	private CompletionStage<ReactiveConnection> getConnectionFromPool(Pool pool) {
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

	private SqlClientConnection newConnection(SqlConnection connection) {
		return new SqlClientConnection( connection, pool, showSQL, formatSQL, usePostgresStyleParameters );
	}

	@Override
	public ReactiveConnection getProxyConnection() {
		return new ProxyConnection();
	}

	@Override
	public ReactiveConnection getProxyConnection(String tenantId) {
		return new ProxyConnection( tenantId );
	}

	@Override
	public void stop() {
		if ( pool != null ) {
			pool.close();
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

	/**
	 * A proxy {@link ReactiveConnection} that initializes the
	 * underlying connection lazily.
	 */
	protected class ProxyConnection implements ReactiveConnection {
		private ReactiveConnection connection;
		private boolean connected;
		private final String tenantId;

		public ProxyConnection() {
			tenantId = null;
		}

		public ProxyConnection(String tenantId) {
			this.tenantId = tenantId;
		}

		private <T> CompletionStage<T> withConnection(Function<ReactiveConnection,CompletionStage<T>> operation) {
			if (!connected) {
				connected = true; // we're not allowed to fetch two connections!
				CompletionStage<ReactiveConnection> connection =
						tenantId==null ? getConnection() : getConnection( tenantId );
				return connection.thenApply( newConnection -> this.connection = newConnection )
						.thenCompose( operation );
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
		public CompletionStage<Void> executeOutsideTransaction(String sql) {
			return withConnection( conn -> conn.executeOutsideTransaction(sql) );
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

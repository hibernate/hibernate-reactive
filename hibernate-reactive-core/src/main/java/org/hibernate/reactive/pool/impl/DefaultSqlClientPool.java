/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.net.URI;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.hibernate.HibernateError;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.Driver;

import static org.hibernate.internal.CoreLogging.messageLogger;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 * The {@code Pool} itself is backed by an instance of {@link Vertx}
 * obtained via the {@link VertxInstance} service. Configuration of
 * the {@code Pool} is handled by the {@link SqlClientPoolConfiguration}
 * service.
 * <p>
 * This class may be extended by programs which wish to implement
 * custom connection management or multitenancy.
 * <p>
 * The lifecycle of this pool is managed by Hibernate Reactive: it
 * is created when the reactive {@link org.hibernate.SessionFactory}
 * is created and destroyed when the {@code SessionFactory} is
 * destroyed. For cases where the underlying {@code Pool} lifecycle
 * is managed externally to Hibernate, use
 * {@link org.hibernate.reactive.pool.impl.ExternalSqlClientPool}.
 *
 * @see SqlClientPoolConfiguration
 */
public class DefaultSqlClientPool extends SqlClientPool
		implements ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	private ThreadLocalPoolManager pools;
	private SqlStatementLogger sqlStatementLogger;
	private URI uri;
	private ServiceRegistryImplementor serviceRegistry;

	public DefaultSqlClientPool() {}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
		sqlStatementLogger = serviceRegistry.getService(JdbcServices.class).getSqlStatementLogger();
	}

	@Override
	public void configure(Map configuration) {
		uri = jdbcUrl( configuration );
	}

	@Override
	public void start() {
		if ( pools == null ) {
			pools = createPools( uri );
		}
	}

	@Override
	protected Pool getPool() {
		return pools.getOrStartPool();
	}

	@Override
	protected SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	/**
	 * Create a new {@link ThreadLocalPoolManager} for the given JDBC URL or database URI,
	 * using the {@link VertxInstance} service to obtain an instance of
	 * {@link Vertx}, and the {@link SqlClientPoolConfiguration} service
	 * to obtain options for creating the connection pool instances.
	 *
	 * @param uri JDBC URL or database URI
	 *
	 * @return the new {@link ThreadLocalPoolManager}
	 */
	protected ThreadLocalPoolManager createPools(URI uri) {
		SqlClientPoolConfiguration configuration = serviceRegistry.getService(SqlClientPoolConfiguration.class);
		VertxInstance vertx = serviceRegistry.getService(VertxInstance.class);
		return createPools( uri, configuration.connectOptions( uri ), configuration.poolOptions(), vertx.getVertx() );
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
	protected ThreadLocalPoolManager createPools(URI uri, SqlConnectOptions connectOptions, PoolOptions poolOptions, Vertx vertx) {
		return new ThreadLocalPoolManager( () -> {
			try {
				// First try to load the Pool using the standard ServiceLoader pattern
				// This only works if exactly 1 Driver is on the classpath.
				return Pool.pool( vertx, connectOptions, poolOptions );
			}
			catch (ServiceConfigurationError e) {
				// Backup option if multiple drivers are on the classpath.
				// We will be able to remove this once Vertx 3.9.2 is available
				final Driver driver = findDriver( uri, e );
				return driver.createPool( vertx, connectOptions, poolOptions );
			}
		});
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
		messageLogger( DefaultSqlClientPool.class ).infof( "HRX000011: SQL Client URL [%s]", url );
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
		for ( Driver d : ServiceLoader.load( Driver.class ) ) {
			String driverName = d.getClass().getCanonicalName();
			messageLogger( DefaultSqlClientPool.class ).infof( "HRX000013: Detected driver [%s]", driverName );
			switch (driverName) {
				case "io.vertx.db2client.spi.DB2Driver":
					if ( "db2".equalsIgnoreCase( scheme ) ) {
						return d;
					}
				case "io.vertx.mysqlclient.spi.MySQLDriver":
					if ( "mysql".equalsIgnoreCase( scheme ) ) {
						return d;
					}
					if ( "mariadb".equalsIgnoreCase( scheme ) ) {
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

	@Override
	public void stop() {
		if ( pools != null ) {
			pools.close();
		}
	}

	public static URI parse(String url) {
		if ( url == null || url.trim().isEmpty() ) {
			throw new HibernateError( "The configuration property '" + Settings.URL + "' was not provided, or is in invalid format. This is required when using the default DefaultSqlClientPool: " +
											  "either provide the configuration setting or integrate with a different SqlClientPool implementation" );
		}

		if ( url.startsWith( "jdbc:" ) ) {
			return URI.create( url.substring( 5 ) );
		}

		return URI.create( url );
	}

}

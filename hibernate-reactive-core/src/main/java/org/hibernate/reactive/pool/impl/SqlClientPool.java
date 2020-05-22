package org.hibernate.reactive.pool.impl;

import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.Driver;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import java.net.URI;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionStage;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 * The {@code Pool} itself is backed by an instance of {@link Vertx}
 * obtained via the {@link VertxInstance} service.
 */
public class SqlClientPool implements ReactiveConnectionPool, ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	public static final int DEFAULT_POOL_SIZE = 5;
	private Pool pool;
	private boolean showSQL;
	private ServiceRegistryImplementor serviceRegistry;
	private Map configurationValues;

	public SqlClientPool() {}

	public SqlClientPool(Pool pool, boolean showSQL) {
		this.pool = pool;
		this.showSQL = showSQL;
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void configure(Map configurationValues) {
		this.showSQL = "true".equals( configurationValues.get( AvailableSettings.SHOW_SQL ) );
		//TODO: actually extract the configuration values we need rather than keeping a reference to the whole map.
		this.configurationValues = configurationValues;
	}

	@Override
	public void start() {
		if ( pool == null ) {
			pool = createPool(configurationValues);
		}
	}

	protected Pool createPool(Map configurationValues) {
		Vertx vertx = serviceRegistry.getService( VertxInstance.class ).getVertx();
		return configurePool( configurationValues, vertx );
	}

	protected Pool configurePool(Map configurationValues, Vertx vertx) {
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

	private URI jdbcUrl(Map configurationValues) {
		final String url = ConfigurationHelper.getString( AvailableSettings.URL, configurationValues );
		CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000011: SQL Client URL [%s]", url );
		return parse( url );
	}

	private PoolOptions poolOptions(Map configurationValues) {
		final int poolSize = ConfigurationHelper.getInt( AvailableSettings.POOL_SIZE, configurationValues, DEFAULT_POOL_SIZE );
		CoreLogging.messageLogger(SqlClientPool.class).infof( "HRX000012: Connection pool size: %d", poolSize );
		return new PoolOptions().setMaxSize( poolSize );
	}

	private SqlConnectOptions sqlConnectOptions(URI uri) {

		String database = uri.getPath().substring( 1 );
		if (uri.getScheme().equals("db2") && database.indexOf( ':' ) > 0) {
			database = database.substring( 0, database.indexOf( ':' ) );
		}

		// FIXME: Check which values can be null
		String username = ConfigurationHelper.getString( AvailableSettings.USER, configurationValues );
		String password = ConfigurationHelper.getString( AvailableSettings.PASS, configurationValues );
		if (username==null || password==null) {
			String[] params = {};
			// DB2 URLs are a bit odd and have the format: jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
			if (uri.getScheme().equals("db2")) {
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

		SqlConnectOptions connectOptions = new SqlConnectOptions()
				.setHost( uri.getHost() )
				.setPort( uri.getPort() )
				.setDatabase( database )
				.setUser( username );
		if (password != null) {
			connectOptions.setPassword( password );
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
										? succeededFuture( new SqlClientConnection( ar.result(), showSQL) )
										: failedFuture( ar.cause() )
						)
				)
		);
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

}

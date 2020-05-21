package org.hibernate.reactive.pool.impl;

import java.net.URI;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionStage;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.cfg.ReactiveSettings;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.service.VertxService;
import org.hibernate.reactive.util.impl.JdbcUrlParser;
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

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 */
public class SqlClientPool implements ReactiveConnectionPool, ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	public static final int DEFAULT_POOL_SIZE = 5;
	private Pool pool;
	private boolean showSQL;
	private ServiceRegistryImplementor serviceRegistry;
	private Map configurationValues;

	public SqlClientPool() {
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void configure(Map configurationValues) {
		this.showSQL = "true".equals(configurationValues.get(AvailableSettings.SHOW_SQL));
		this.configurationValues = configurationValues;
	}

	@Override
	public void start() {
		Object o = configurationValues.get( ReactiveSettings.VERTX_POOL );
		if (o != null) {
			if (!(o instanceof Pool)) {
				throw new ConfigurationException("Setting " + ReactiveSettings.VERTX_POOL + " must be configured with an instance of " +
						Pool.class.getCanonicalName() + " but was configured with " + o);
			} else {
				pool = (Pool) o;
			}
		} else {
			final VertxService vertxService = serviceRegistry.getService( VertxService.class );
			Vertx vertx = vertxService.getVertx();
			pool = configurePool( configurationValues, vertx );
		}

	}

	private Pool configurePool(Map configurationValues, Vertx vertx) {
		// FIXME: Check which values can be null
		String username = ConfigurationHelper.getString(AvailableSettings.USER, configurationValues);
		String password = ConfigurationHelper.getString(AvailableSettings.PASS, configurationValues);

		final int poolSize = ConfigurationHelper.getInt(AvailableSettings.POOL_SIZE, configurationValues, DEFAULT_POOL_SIZE);

		final String url = ConfigurationHelper.getString(AvailableSettings.URL, configurationValues);
		final URI uri = JdbcUrlParser.parse( url );
		String database = uri.getPath().substring( 1 );
		if (uri.getScheme().equals("db2") && database.indexOf( ':' ) > 0) {
			database = database.substring( 0, database.indexOf( ':' ) );
		}

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

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize( poolSize );
        SqlConnectOptions connectOptions = new SqlConnectOptions()
                .setHost( uri.getHost() )
                .setPort( uri.getPort() )
                .setDatabase( database )
                .setUser( username );
        if (password != null) {
            connectOptions.setPassword( password );
        }
        
        // First try to load the Pool using the standard ServiceLoader pattern
        // This only works if exactly 1 Driver is on the classpath.
        ServiceConfigurationError originalError;
        try {
            return Pool.pool( vertx, connectOptions, poolOptions );
        } catch (ServiceConfigurationError e) {
            originalError = e;
        }
        
        // Backup option if multiple drivers are on the classpath. 
        // We will be able to remove this once Vertx 3.9.2 is available
        String scheme = uri.getScheme(); // "postgresql", "mysql", "db2", etc
        for (Driver d : ServiceLoader.load( Driver.class )) {
            String driverName = d.getClass().getCanonicalName();
            if ("io.vertx.db2client.spi.DB2Driver".equals( driverName ) && "db2".equalsIgnoreCase( scheme )) {
                return d.createPool( vertx, connectOptions, poolOptions );
            }
            if ("io.vertx.mysqlclient.spi.MySQLDriver".equals( driverName ) && "mysql".equalsIgnoreCase( scheme )) {
                return d.createPool( vertx, connectOptions, poolOptions );
            }
            if ("io.vertx.pgclient.spi.PgDriver".equals( driverName ) && 
                    ("postgre".equalsIgnoreCase( scheme ) ||
                     "postgres".equalsIgnoreCase( scheme ) ||
                     "postgresql".equalsIgnoreCase( scheme ))) {
                return d.createPool( vertx, connectOptions, poolOptions );
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
		this.pool.close();
	}

}

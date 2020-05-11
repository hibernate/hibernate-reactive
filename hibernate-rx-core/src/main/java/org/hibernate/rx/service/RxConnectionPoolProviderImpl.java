package org.hibernate.rx.service;

import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.rx.cfg.AvailableRxSettings;
import org.hibernate.rx.impl.PoolConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.util.impl.JdbcUrlParser;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Stoppable;

import java.net.URI;
import java.util.Map;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 */
public class RxConnectionPoolProviderImpl implements RxConnectionPoolProvider, Configurable, Stoppable {

	public static final int DEFAULT_POOL_SIZE = 5;
	private Pool pool;
	private boolean showSQL;

	public RxConnectionPoolProviderImpl(Map configurationValues) {
		configure( configurationValues );
	}

	@Override
	public void configure(Map configurationValues) {
		Object o = configurationValues.get( AvailableRxSettings.VERTX_POOL );
		if (o != null) {
			if (!(o instanceof Pool)) {
				throw new ConfigurationException("Setting " + AvailableRxSettings.VERTX_POOL + " must be configured with an instance of " +
						Pool.class.getCanonicalName() + " but was configured with " + o);
			} else {
				pool = (Pool) o;
			}
		} else {
			pool = configurePool( configurationValues );
		}

		showSQL = "true".equals( configurationValues.get( AvailableSettings.SHOW_SQL ) );
	}
	
	private Pool configurePool(Map configurationValues) {
		// FIXME: Check which values can be null
		String username = ConfigurationHelper.getString(AvailableSettings.USER, configurationValues);
		String password = ConfigurationHelper.getString(AvailableSettings.PASS, configurationValues);

		final Integer poolSize = ConfigurationHelper.getInt(AvailableSettings.POOL_SIZE, configurationValues, DEFAULT_POOL_SIZE);

		final String url = ConfigurationHelper.getString(AvailableSettings.URL, configurationValues);
		final URI uri = JdbcUrlParser.parse( url );
		final String database = uri.getPath().substring( 1 );

		if (username==null || password==null) {
			String[] params = uri.getQuery().split("&");
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
		switch ( uri.getScheme() ) {
			case "postgresql":
				PgConnectOptions pgOptions = new PgConnectOptions()
						.setPort( uri.getPort() )
						.setHost( uri.getHost() )
						.setDatabase( database )
						.setUser( username );
				if (password != null) {
					pgOptions.setPassword( password );
				}
				return PgPool.pool(Vertx.vertx(), pgOptions, poolOptions);
			case "mysql":
				MySQLConnectOptions mysqlOptions = new MySQLConnectOptions()
						.setPort( uri.getPort() )
						.setHost( uri.getHost() )
						.setDatabase( database )
						.setUser( username );
				if (password != null) {
					mysqlOptions.setPassword( password );
				}
				return MySQLPool.pool(Vertx.vertx(), mysqlOptions, poolOptions);
			default:
				throw new ConfigurationException( "Unrecognized URI scheme: " + uri.getScheme() );	
		}
	}

	@Override
	public RxConnection getConnection() {
		return new PoolConnection( pool, showSQL );
	}

	@Override
	public void close() {
		this.pool.close();
	}

	@Override
	public void stop() {
		close();
	}
}

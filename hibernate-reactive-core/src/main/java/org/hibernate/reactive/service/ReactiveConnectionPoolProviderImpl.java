package org.hibernate.reactive.service;

import java.net.URI;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.cfg.ReactiveSettings;
import org.hibernate.reactive.impl.SqlClientConnection;
import org.hibernate.reactive.impl.TransactionalSqlClientConnection;
import org.hibernate.reactive.service.initiator.ReactiveConnectionPoolProvider;
import org.hibernate.reactive.util.impl.JdbcUrlParser;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Stoppable;

import io.vertx.core.Vertx;
import io.vertx.db2client.DB2ConnectOptions;
import io.vertx.db2client.DB2Pool;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 */
public class ReactiveConnectionPoolProviderImpl implements ReactiveConnectionPoolProvider, Configurable, Stoppable {

	public static final int DEFAULT_POOL_SIZE = 5;
	private Pool pool;
	private boolean showSQL;

	public ReactiveConnectionPoolProviderImpl(Map configurationValues) {
		configure( configurationValues );
	}

	@Override
	public void configure(Map configurationValues) {
		Object o = configurationValues.get( ReactiveSettings.VERTX_POOL );
		if (o != null) {
			if (!(o instanceof Pool)) {
				throw new ConfigurationException("Setting " + ReactiveSettings.VERTX_POOL + " must be configured with an instance of " +
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
			case "db2":
			    DB2ConnectOptions db2Options = new DB2ConnectOptions()
			            .setPort( uri.getPort() )
			            .setHost( uri.getHost() )
			            .setDatabase( database )
			            .setUser( username );
			    if (password != null) {
			        db2Options.setPassword( password );
			    }
			    return DB2Pool.pool(Vertx.vertx(), db2Options, poolOptions);
			default:
				throw new ConfigurationException( "Unrecognized URI scheme: " + uri.getScheme() );	
		}
	}

	@Override
	public ReactiveConnection getConnection() {
		return new SqlClientConnection( pool, showSQL );
	}

	@Override
	public ReactiveConnection getTransactionalConnection() {
		return new TransactionalSqlClientConnection( pool, showSQL );
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

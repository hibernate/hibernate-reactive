package org.hibernate.rx.service;

import java.net.URI;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.rx.util.impl.JdbcUrlParser;
import org.hibernate.rx.impl.PgPoolConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Stoppable;

import io.vertx.axle.core.Vertx;
import io.vertx.axle.pgclient.PgPool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

public class RxConnectionPoolProviderImpl implements RxConnectionPoolProvider, Configurable, Stoppable {

	public static final int DEFAULT_POOL_SIZE = 5;
	private PgPool pool;
	private boolean showSQL;

	public RxConnectionPoolProviderImpl() {
	}

	public RxConnectionPoolProviderImpl(Map configurationValues) {
		configure( configurationValues );
	}

	private Integer poolSize(Map configurationValues) {
		Integer poolSize = (Integer) configurationValues.get( AvailableSettings.POOL_SIZE );
		return poolSize != null ? poolSize : DEFAULT_POOL_SIZE;
	}

	@Override
	public void configure(Map configurationValues) {
		// FIXME: Check which values can be null
		String username = (String) configurationValues.get( AvailableSettings.USER );
		String password = (String) configurationValues.get( AvailableSettings.PASS );
		final String url = (String) configurationValues.get( AvailableSettings.URL );
		final URI uri = JdbcUrlParser.parse( url );
		final Integer poolSize = poolSize( configurationValues );
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
		PgConnectOptions connectOptions = new PgConnectOptions()
				.setPort( uri.getPort() )
				.setHost( uri.getHost() )
				.setDatabase( database )
				.setUser( username )
				.setPassword( password );
		this.pool = PgPool.pool( Vertx.vertx(), connectOptions, poolOptions );

		showSQL = "true".equals( configurationValues.get( AvailableSettings.SHOW_SQL ) );
	}

	@Override
	public RxConnection getConnection() {
		return new PgPoolConnection( pool, showSQL );
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

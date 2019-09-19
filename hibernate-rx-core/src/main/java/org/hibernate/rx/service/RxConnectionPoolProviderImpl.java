package org.hibernate.rx.service;

import java.net.URI;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.rx.configuration.JdbcUrlParser;
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
		final String username = (String) configurationValues.get( AvailableSettings.USER );
		final String password = (String) configurationValues.get( AvailableSettings.PASS );
		final String url = (String) configurationValues.get( AvailableSettings.URL );
		final URI uri = JdbcUrlParser.parse( url );
		final Integer poolSize = poolSize( configurationValues );
		final String database = uri.getPath().substring( 1 );

		PoolOptions poolOptions = new PoolOptions()
				.setMaxSize( poolSize );
		PgConnectOptions connectOptions = new PgConnectOptions()
				.setPort( uri.getPort() )
				.setHost( uri.getHost() )
				.setDatabase( database )
				.setUser( username )
				.setPassword( password );
		this.pool = PgPool.pool( Vertx.vertx(), connectOptions, poolOptions );
	}

	@Override
	public RxConnection getConnection() {
		return new PgPoolConnection( pool );
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

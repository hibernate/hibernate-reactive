package org.hibernate.rx.service;

import java.net.URI;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.rx.configuration.JdbcUrlParser;
import org.hibernate.rx.impl.PgPoolConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.service.spi.Configurable;

import io.reactiverse.pgclient.PgPoolOptions;

public class RxConnectionPoolProviderImpl implements RxConnectionPoolProvider, Configurable {

	public static final int DEFAULT_POOL_SIZE = 5;
	private PgPoolOptions options;

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
		final String username = (String) configurationValues.get( AvailableSettings.USER );
		final String password = (String) configurationValues.get( AvailableSettings.PASS );
		final String database = (String) configurationValues.get( AvailableSettings.DATASOURCE );
		final String url = (String) configurationValues.get( AvailableSettings.URL );
		final URI uri = JdbcUrlParser.parse( url );
		final Integer poolSize = poolSize( configurationValues );

		options = new PgPoolOptions()
				.setPort( uri.getPort() )
				.setHost( uri.getHost() )
				.setDatabase( database )
				.setUser( username )
				.setPassword( password )
				.setMaxSize( poolSize );
	}

	@Override
	public RxConnection getConnection() {
		return new PgPoolConnection( options );
	}
}

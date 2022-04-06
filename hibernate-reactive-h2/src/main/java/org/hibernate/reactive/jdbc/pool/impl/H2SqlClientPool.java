/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;


import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;

public class H2SqlClientPool extends SqlClientPool
		implements ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public static final String DEFAULT_URL = "jdbc:h2:mem:hreact";

	//Asynchronous shutdown promise: we can't return it from #close as we implement a
	//blocking interface.
	private volatile Future<Void> closeFuture = Future.succeededFuture();

	private Pool pools;
	private URI uri;
	private SqlStatementLogger sqlStatementLogger;
	private ServiceRegistryImplementor serviceRegistry;

	public H2SqlClientPool() {
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
		sqlStatementLogger = serviceRegistry.getService( JdbcServices.class ).getSqlStatementLogger();
	}

	@Override
	public void configure(Map configuration) {
		uri = jdbcUrl( configuration );
	}

	protected URI jdbcUrl(Map<?,?> configurationValues) {
		String url = ConfigurationHelper.getString( Settings.URL, configurationValues, DEFAULT_URL );
		LOG.sqlClientUrl( url);
		return URI.create( url );
	}

	@Override
	public void start() {
		if ( pools == null ) {
			pools = createPool( uri );
		}
	}

	@Override
	public CompletionStage<Void> getCloseFuture() {
		return closeFuture.toCompletionStage();
	}

	@Override
	protected Pool getPool() {
		return pools;
	}

	private Pool createPool(URI uri) {
		JdbcClientPoolConfiguration configuration = serviceRegistry.getService( JdbcClientPoolConfiguration.class );
		VertxInstance vertx = serviceRegistry.getService( VertxInstance.class );
		JsonObject poolOptions = configuration.poolOptions().toJson();
		JsonObject connectOptions = configuration.jdbcConnectOptions( uri );
		JsonObject config = poolOptions.mergeIn( connectOptions );
		return JDBCPool.pool( vertx.getVertx(), config );
	}

	@Override
	public void stop() {
		if ( pools != null ) {
			this.closeFuture = pools.close();
		}
	}

	@Override
	protected SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}
}

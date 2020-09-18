/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider;

import org.hibernate.cfg.AvailableSettings;

/**
 * Configuration properties for the Hibernate Reactive persistence provider,
 * for use with {@link org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder}.
 *
 * @author Gavin King
 */
public interface Settings extends AvailableSettings {

	/**
	 * Property for configuring the Vert.x prepared statement cache.
	 *
	 * @see io.vertx.sqlclient.SqlConnectOptions#setPreparedStatementCacheSqlLimit(int)
	 */
	String PREPARED_STATEMENT_CACHE_SQL_LIMIT = "hibernate.vertx.prepared_statement_cache.sql_limit";

	/**
	 * Property for configuring the Vert.x prepared statement cache.
	 *
	 * @see io.vertx.sqlclient.SqlConnectOptions#setPreparedStatementCacheMaxSize(int)
	 */
	String PREPARED_STATEMENT_CACHE_MAX_SIZE = "hibernate.vertx.prepared_statement_cache.max_size";

	/**
	 * Property for configuring the Vert.x connection pool.
	 *
	 * @see io.vertx.sqlclient.PoolOptions#setMaxWaitQueueSize(int)
	 */
	String POOL_MAX_WAIT_QUEUE_SIZE = "hibernate.vertx.pool.max_wait_queue_size";

	/**
	 * Property for configuring the Vert.x connection pool.
	 *
	 * @see io.vertx.sqlclient.SqlConnectOptions#setConnectTimeout(int)
	 */
	String POOL_CONNECT_TIMEOUT = "hibernate.vertx.pool.connect_timeout";

	/**
	 * Property for configuring the Vert.x connection pool.
	 *
	 * @see io.vertx.sqlclient.SqlConnectOptions#setIdleTimeout(int)
	 */
	String POOL_IDLE_TIMEOUT = "hibernate.vertx.pool.idle_timeout";

	/**
	 * Specifies a {@link org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration} class.
	 */
	String SQL_CLIENT_POOL_CONFIG = "hibernate.vertx.pool.configuration_class";

	/**
	 * Specifies a {@link org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration} class.
	 */
	String SQL_CLIENT_POOL = "hibernate.vertx.pool.class";
}

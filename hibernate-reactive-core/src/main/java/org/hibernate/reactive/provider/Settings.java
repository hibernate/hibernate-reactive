/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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
	 * @see io.vertx.sqlclient.PoolOptions#setConnectionTimeout(int)
	 */
	String POOL_CONNECT_TIMEOUT = "hibernate.vertx.pool.connect_timeout";

	/**
	 * Property for configuring the Vert.x connection pool.
	 *
	 * @see io.vertx.sqlclient.PoolOptions#setIdleTimeout(int)
	 */
	String POOL_IDLE_TIMEOUT = "hibernate.vertx.pool.idle_timeout";

	/**
	 * Property for configuring the Vert.x connection pool.
	 *
	 * @see io.vertx.sqlclient.PoolOptions#setPoolCleanerPeriod(int)
	 */
	String POOL_CLEANER_PERIOD = "hibernate.vertx.pool.cleaner_period";

	/**
	 * Specifies a {@link org.hibernate.reactive.pool.internal.SqlClientPoolConfiguration} class.
	 */
	String SQL_CLIENT_POOL_CONFIG = "hibernate.vertx.pool.configuration_class";

	/**
	 * Specifies a {@link org.hibernate.reactive.pool.ReactiveConnectionPool} class.
	 */
	String SQL_CLIENT_POOL = "hibernate.vertx.pool.class";

	/**
	 * When {@code true}, Hibernate Reactive won't check during bootstrap that
	 * the versions of Hibernate ORM and the Vert.x SQL client on the classpath are not older
	 * than the minimum versions it requires.
	 * <p>
	 * It can also be set as a system property, for example from the command line
	 * with {@code -Dhibernate.reactive.skip_version_check=true}. When set, the system
	 * property has precedence over the value in the configuration.
	 * <p>
	 * Default: {@code false}
	 */
	String SKIP_VERSION_CHECK = "hibernate.reactive.skip_version_check";
}

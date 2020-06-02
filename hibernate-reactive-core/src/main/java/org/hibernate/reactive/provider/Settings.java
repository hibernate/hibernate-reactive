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
	String MAX_WAIT_QUEUE_SIZE = "hibernate.vertx.pool.max_wait_queue_size";

}

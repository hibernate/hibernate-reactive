/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.hibernate.service.Service;

import java.net.URI;

/**
 * A strategy for configuring the Vert.x {@link io.vertx.sqlclient.Pool}
 * used by {@link SqlClientPool}.
 * <p>
 * A custom strategy may be selected using the configuration property
 * {@link org.hibernate.reactive.provider.Settings#SQL_CLIENT_POOL_CONFIG}.
 */
public interface SqlClientPoolConfiguration extends Service {
    /**
     * The {@link PoolOptions} used to configure the {@code Pool}
     */
    PoolOptions poolOptions();
    /**
     * The {@link SqlConnectOptions} used to configure the {@code Pool}
     *
     * @param uri A {@link URI} representing the JDBC URL or connection URI
     *            specified in the configuration properties, usually via
     *            {@link org.hibernate.cfg.Environment#JPA_JDBC_URL}, or
     *            {@code null} if not specified.
     */
    SqlConnectOptions connectOptions(URI uri);
}

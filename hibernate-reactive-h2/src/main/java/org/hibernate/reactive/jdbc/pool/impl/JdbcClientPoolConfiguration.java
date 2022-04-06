/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.net.URI;

import org.hibernate.service.Service;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.PoolOptions;

public interface JdbcClientPoolConfiguration extends Service {
	/**
	 * The {@link PoolOptions} used to configure the {@code Pool}
	 */
	PoolOptions poolOptions();

	/**
	 * The {@link JsonObject} used to configure the {@code Pool}
	 *
	 * @param uri A {@link URI} representing the JDBC URL or connection URI
	 * specified in the configuration properties, usually via
	 * {@link org.hibernate.cfg.Environment#JPA_JDBC_URL}, or
	 * {@code null} if not specified.
	 */
	JsonObject jdbcConnectOptions(URI uri);
}

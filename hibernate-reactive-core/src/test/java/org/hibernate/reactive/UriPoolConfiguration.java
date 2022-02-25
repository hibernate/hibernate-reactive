/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.net.URI;

import org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration;

import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

public class UriPoolConfiguration implements SqlClientPoolConfiguration {
	@Override
	public PoolOptions poolOptions() {
		return new PoolOptions();
	}

	@Override
	public SqlConnectOptions connectOptions(URI uri) {
		return SqlConnectOptions.fromUri( uri.toString() );
	}
}

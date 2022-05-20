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

/**
 * This class is used by {@link UriConfigTest} to test that one can use a different implementation of
 * {@link SqlClientPoolConfiguration}.
 * <p>
 * But, in reality, it also checks {@link SqlConnectOptions#fromUri(String)} works for each database.
 *</p>
 */
public class UriPoolConfiguration implements SqlClientPoolConfiguration {
	@Override
	public PoolOptions poolOptions() {
		return new PoolOptions();
	}

	@Override
	public SqlConnectOptions connectOptions(URI uri) {
		// For CockroachDB we use the PostgreSQL Vert.x client
		String uriString = uri.toString().replaceAll( "^cockroach(db)?:", "postgres:" );
		if ( uriString.startsWith( "sqlserver" ) ) {
			// Testscontainer adds encrypt=false to the url. The problem is that it uses the JDBC syntax that's
			// different from the supported one by the Vert.x driver.
			uriString = uriString.replaceAll( ";encrypt=false", "?encrypt=false" );
		}
		return SqlConnectOptions.fromUri( uriString );
	}
}

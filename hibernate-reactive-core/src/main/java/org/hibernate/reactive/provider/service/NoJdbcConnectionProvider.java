/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.reactive.logging.impl.Log;

import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * A dummy Hibernate {@link ConnectionProvider} throws an
 * exception if a JDBC connection is requested.
 *
 * @author Gavin King
 */
public class NoJdbcConnectionProvider implements ConnectionProvider {
	private static final Log LOG = make( Log.class, lookup() );

	public static final NoJdbcConnectionProvider INSTANCE = new NoJdbcConnectionProvider();

	@Override
	public Connection getConnection() throws SQLException {
		throw LOG.notUsingJdbc();
	}

	@Override
	public void closeConnection(Connection conn) {}

	@Override
	public boolean supportsAggressiveRelease() {
		return false;
	}

	@Override
	public boolean isUnwrappableAs(Class unwrapType) {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> unwrapType) {
		return null;
	}
}

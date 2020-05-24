package org.hibernate.reactive.boot.service;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A dummy Hibernate {@link ConnectionProvider} throws an
 * exception if a JDBC connection is requested.
 *
 * @author Gavin King
 */
public class NoJdbcConnectionProvider implements ConnectionProvider {

	public static final NoJdbcConnectionProvider INSTANCE = new NoJdbcConnectionProvider();

	@Override
	public Connection getConnection() throws SQLException {
		throw new SQLException("Not using JDBC");
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

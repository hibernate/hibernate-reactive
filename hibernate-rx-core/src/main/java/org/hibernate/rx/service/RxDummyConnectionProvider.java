package org.hibernate.rx.service;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

public class RxDummyConnectionProvider implements ConnectionProvider {
	@Override
	public Connection getConnection() throws SQLException {
		throw new SQLException("Not using JDBC");
	}

	@Override
	public void closeConnection(Connection conn)  {}

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

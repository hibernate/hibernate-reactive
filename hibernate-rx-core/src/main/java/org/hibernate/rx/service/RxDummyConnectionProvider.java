package org.hibernate.rx.service;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.exception.JDBCConnectionException;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Stoppable;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * A wrapper for the Hibernate {@link ConnectionProvider} that simply
 * squashes any errors that happen at startup time if there is no
 * JDBC driver available.
 *
 * @author Gavin King
 */
public class RxDummyConnectionProvider implements ConnectionProvider, Configurable, Stoppable, ServiceRegistryAwareService {
	private boolean noJDBC;
	private ConnectionProvider delegate;
	private ServiceRegistryImplementor registry;

	public RxDummyConnectionProvider(ConnectionProvider delegate) {
		this.delegate = delegate;
	}

	@Override
	public Connection getConnection() throws SQLException {
		if (noJDBC) {
			throw new SQLException("Not using JDBC");
		}
		else {
			return delegate.getConnection();
		}
	}

	@Override
	public void closeConnection(Connection conn) throws SQLException {
		delegate.closeConnection(conn);
	}

	@Override
	public boolean supportsAggressiveRelease() {
		return delegate.supportsAggressiveRelease();
	}

	@Override
	public void configure(Map configurationValues) {
		if (delegate instanceof Configurable) {
			try {
				((Configurable) delegate).configure(configurationValues);
			}
			catch (JDBCConnectionException e) {
				noJDBC = true;
				registry.getService( SchemaManagementTool.class )
						.setCustomDatabaseGenerationTarget( new RxGenerationTarget(registry) );
			}
		}
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.registry = serviceRegistry;
		if (delegate instanceof ServiceRegistryAwareService) {
			((ServiceRegistryAwareService) delegate).injectServices(serviceRegistry);
		}
	}

	@Override
	public void stop() {
		if (!noJDBC && delegate instanceof Stoppable) {
			((Stoppable) delegate).stop();
		}
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

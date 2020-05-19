package org.hibernate.reactive.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.jdbc.connections.internal.ConnectionProviderInitiator;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.reactive.service.ReactiveDummyConnectionProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * wraps the Hibernate {@link ConnectionProvider} in an instance of
 * {@link ReactiveDummyConnectionProvider}.
 *
 * @author Gavin King
 */
public class DummyConnectionProviderInitiator implements StandardServiceInitiator<ConnectionProvider> {

	public static final DummyConnectionProviderInitiator INSTANCE = new DummyConnectionProviderInitiator();

	@Override
	public ConnectionProvider initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
			return new ReactiveDummyConnectionProvider(
					ConnectionProviderInitiator.INSTANCE.initiateService(configurationValues, registry)
			);
	}

	@Override
	public Class<ConnectionProvider> getServiceInitiated() {
		return ConnectionProvider.class;
	}
}

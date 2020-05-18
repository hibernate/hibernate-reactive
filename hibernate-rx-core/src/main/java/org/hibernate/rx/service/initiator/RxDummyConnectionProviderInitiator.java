package org.hibernate.rx.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.jdbc.connections.internal.ConnectionProviderInitiator;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.rx.service.RxDummyConnectionProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

public class RxDummyConnectionProviderInitiator implements StandardServiceInitiator<ConnectionProvider> {

	public static final RxDummyConnectionProviderInitiator INSTANCE = new RxDummyConnectionProviderInitiator();

	@Override
	public ConnectionProvider initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
			return new RxDummyConnectionProvider(
					ConnectionProviderInitiator.INSTANCE.initiateService(configurationValues, registry)
			);
	}

	@Override
	public Class<ConnectionProvider> getServiceInitiated() {
		return ConnectionProvider.class;
	}
}

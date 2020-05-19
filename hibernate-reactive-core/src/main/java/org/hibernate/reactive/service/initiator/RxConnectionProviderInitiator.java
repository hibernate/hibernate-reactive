package org.hibernate.reactive.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.service.RxConnectionPoolProviderImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our {@link RxConnectionPoolProvider}.
 *
 * @see RxConnectionPoolProvider
 * @see RxConnectionPoolProviderImpl
 */
public class RxConnectionProviderInitiator implements StandardServiceInitiator<RxConnectionPoolProviderImpl> {

	public static final RxConnectionProviderInitiator INSTANCE = new RxConnectionProviderInitiator();

	private RxConnectionProviderInitiator() {
	}

	@Override
	public RxConnectionPoolProviderImpl initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxConnectionPoolProviderImpl( configurationValues );
	}

	@Override
	public Class<RxConnectionPoolProviderImpl> getServiceInitiated() {
		return RxConnectionPoolProviderImpl.class;
	}
}

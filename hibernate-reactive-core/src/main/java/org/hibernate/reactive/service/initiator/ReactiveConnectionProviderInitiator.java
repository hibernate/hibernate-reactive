package org.hibernate.reactive.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.service.ReactiveConnectionPoolProvider;
import org.hibernate.reactive.service.ReactiveConnectionPoolProviderImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our {@link ReactiveConnectionPoolProvider}.
 *
 * @see ReactiveConnectionPoolProvider
 * @see ReactiveConnectionPoolProviderImpl
 */
public class ReactiveConnectionProviderInitiator implements StandardServiceInitiator<ReactiveConnectionPoolProviderImpl> {

	public static final ReactiveConnectionProviderInitiator INSTANCE = new ReactiveConnectionProviderInitiator();

	private ReactiveConnectionProviderInitiator() {
	}

	@Override
	public ReactiveConnectionPoolProviderImpl initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveConnectionPoolProviderImpl( configurationValues );
	}

	@Override
	public Class<ReactiveConnectionPoolProviderImpl> getServiceInitiated() {
		return ReactiveConnectionPoolProviderImpl.class;
	}
}

package org.hibernate.reactive.jpa.impl;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class RxPersisterClassResolverInitiator implements StandardServiceInitiator<PersisterClassResolver> {
	public static final RxPersisterClassResolverInitiator INSTANCE = new RxPersisterClassResolverInitiator();

	@Override
	public Class<PersisterClassResolver> getServiceInitiated() {
		return PersisterClassResolver.class;
	}

	@Override
	public PersisterClassResolver initiateService(
			Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxPersisterClassResolver();
	}
}
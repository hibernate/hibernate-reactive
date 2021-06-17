/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that creates a {@link ReactivePersisterClassResolver} to register
 * the persisters Hibernate Reactive needs.
 */
public class ReactivePersisterClassResolverInitiator implements StandardServiceInitiator<PersisterClassResolver> {
	public static final ReactivePersisterClassResolverInitiator INSTANCE = new ReactivePersisterClassResolverInitiator();

	@Override
	public Class<PersisterClassResolver> getServiceInitiated() {
		return PersisterClassResolver.class;
	}

	@Override
	public PersisterClassResolver initiateService(
			Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactivePersisterClassResolver();
	}
}

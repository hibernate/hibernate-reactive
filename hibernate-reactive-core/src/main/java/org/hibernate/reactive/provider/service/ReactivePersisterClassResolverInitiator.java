/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.service.spi.ServiceRegistryImplementor;

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

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducerProvider;

public class ReactiveValuesMappingProducerProviderInitiator implements StandardServiceInitiator<JdbcValuesMappingProducerProvider> {
	/**
	 * Singleton access
	 */
	public static final ReactiveValuesMappingProducerProviderInitiator INSTANCE = new ReactiveValuesMappingProducerProviderInitiator();

	@Override
	public JdbcValuesMappingProducerProvider initiateService(
			Map<String, Object> configurationValues,
			ServiceRegistryImplementor registry) {
		return ReactiveValuesMappingProducerProvider.INSTANCE;
	}

	@Override
	public Class<JdbcValuesMappingProducerProvider> getServiceInitiated() {
		return JdbcValuesMappingProducerProvider.class;
	}

}

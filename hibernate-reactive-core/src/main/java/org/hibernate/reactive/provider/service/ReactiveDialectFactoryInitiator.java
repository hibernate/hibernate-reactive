/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveDialectFactoryInitiator  implements StandardServiceInitiator<DialectFactory> {
	/**
	 * Singleton access
	 */
	public static final ReactiveDialectFactoryInitiator INSTANCE = new ReactiveDialectFactoryInitiator();

	@Override
	public Class<DialectFactory> getServiceInitiated() {
		return DialectFactory.class;
	}

	@Override
	public DialectFactory initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveWrappingDialectFactory();
	}
}

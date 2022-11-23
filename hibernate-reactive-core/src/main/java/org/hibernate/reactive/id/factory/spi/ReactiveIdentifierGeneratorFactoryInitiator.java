/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.factory.spi;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.id.factory.IdentifierGeneratorFactory;
import org.hibernate.reactive.id.impl.ReactiveIdentifierGeneratorFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveIdentifierGeneratorFactoryInitiator implements StandardServiceInitiator<IdentifierGeneratorFactory> {
	public static final ReactiveIdentifierGeneratorFactoryInitiator INSTANCE = new ReactiveIdentifierGeneratorFactoryInitiator();

	@Override
	public Class<IdentifierGeneratorFactory> getServiceInitiated() {
		return IdentifierGeneratorFactory.class;
	}

	@Override
	public IdentifierGeneratorFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveIdentifierGeneratorFactory( registry );
	}
}

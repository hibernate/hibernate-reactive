/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.factory.spi;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.id.factory.spi.MutableIdentifierGeneratorFactory;
import org.hibernate.reactive.id.impl.ReactiveIdentifierGeneratorFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

public class ReactiveIdentifierGeneratorFactoryInitiator implements StandardServiceInitiator<MutableIdentifierGeneratorFactory> {
	public static final ReactiveIdentifierGeneratorFactoryInitiator INSTANCE = new ReactiveIdentifierGeneratorFactoryInitiator();

	@Override
	public Class<MutableIdentifierGeneratorFactory> getServiceInitiated() {
		return MutableIdentifierGeneratorFactory.class;
	}

	@Override
	public MutableIdentifierGeneratorFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveIdentifierGeneratorFactory( registry );
	}
}

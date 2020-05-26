/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.id.factory.spi.MutableIdentifierGeneratorFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * @author Gavin King
 */
public class ReactiveIdentifierGeneratorFactoryInitiator implements StandardServiceInitiator<MutableIdentifierGeneratorFactory> {
	public static final ReactiveIdentifierGeneratorFactoryInitiator INSTANCE = new ReactiveIdentifierGeneratorFactoryInitiator();

	@Override
	public Class<MutableIdentifierGeneratorFactory> getServiceInitiated() {
		return MutableIdentifierGeneratorFactory.class;
	}

	@Override
	public MutableIdentifierGeneratorFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveIdentifierGeneratorFactory();
	}
}

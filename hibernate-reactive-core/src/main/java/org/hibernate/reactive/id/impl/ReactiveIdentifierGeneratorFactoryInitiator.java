/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
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

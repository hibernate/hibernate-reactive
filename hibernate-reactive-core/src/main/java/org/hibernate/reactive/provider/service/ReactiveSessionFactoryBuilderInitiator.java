/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.spi.SessionFactoryBuilderService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveSessionFactoryBuilderInitiator implements StandardServiceInitiator<SessionFactoryBuilderService> {

	public static final ReactiveSessionFactoryBuilderInitiator INSTANCE = new ReactiveSessionFactoryBuilderInitiator();

	private ReactiveSessionFactoryBuilderInitiator() {
	}

	@Override
	public SessionFactoryBuilderService initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveSessionFactoryBuilderService();
	}

	@Override
	public Class<SessionFactoryBuilderService> getServiceInitiated() {
		return SessionFactoryBuilderService.class;
	}

}

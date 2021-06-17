/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.spi.SessionFactoryBuilderService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * wraps the Hibernate {@link org.hibernate.engine.jdbc.connections.spi.ConnectionProvider} in an instance of
 * {@link NoJdbcConnectionProvider}.
 */
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

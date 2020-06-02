/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.jdbc.connections.internal.ConnectionProviderInitiator;
import org.hibernate.engine.jdbc.connections.internal.DriverManagerConnectionProviderImpl;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * wraps the Hibernate {@link ConnectionProvider} in an instance of
 * {@link NoJdbcConnectionProvider}.
 *
 * @author Gavin King
 */
public class NoJdbcConnectionProviderInitiator implements StandardServiceInitiator<ConnectionProvider> {

	public static final NoJdbcConnectionProviderInitiator INSTANCE = new NoJdbcConnectionProviderInitiator();

	@Override
	public ConnectionProvider initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		ConnectionProvider provider = ConnectionProviderInitiator.INSTANCE.initiateService(configurationValues, registry);
		if (provider instanceof DriverManagerConnectionProviderImpl) {
			registry.getService( SchemaManagementTool.class )
					.setCustomDatabaseGenerationTarget( new ReactiveGenerationTarget(registry) );
			return NoJdbcConnectionProvider.INSTANCE;
		}
		return provider;
	}

	@Override
	public Class<ConnectionProvider> getServiceInitiated() {
		return ConnectionProvider.class;
	}
}

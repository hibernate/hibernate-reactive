/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.jdbc.pool.impl;

import java.util.Map;

import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.ReactiveConnectionPoolInitiator;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class H2ReactiveConnectionPoolInitiator extends ReactiveConnectionPoolInitiator {

	public static final H2ReactiveConnectionPoolInitiator INSTANCE = new H2ReactiveConnectionPoolInitiator();

	@Override
	public ReactiveConnectionPool initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		String url = ConfigurationHelper.getString( Settings.URL, configurationValues );
		// Check URL for H2 and return H2 specific pool
		if ( url.startsWith( "jdbc:h2:" ) ) {
			return new H2SqlClientPool();
		}

		// delegate to super class to initiate the DefaultSqlClientPool
		return super.initiateService( configurationValues, registry );
	}
}

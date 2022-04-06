/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.pool.impl.H2ClientPoolConfiguration;
import org.hibernate.reactive.pool.impl.JdbcClientPoolConfiguration;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class JdbcSqlClientPoolConfigurationInitiator implements StandardServiceInitiator<JdbcClientPoolConfiguration> {


	public static final JdbcSqlClientPoolConfigurationInitiator INSTANCE = new JdbcSqlClientPoolConfigurationInitiator() {
	};

	@Override
	public JdbcClientPoolConfiguration initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
			return new H2ClientPoolConfiguration();
	}

	@Override
	public Class<JdbcClientPoolConfiguration> getServiceInitiated() {
		return JdbcClientPoolConfiguration.class;
	}
}

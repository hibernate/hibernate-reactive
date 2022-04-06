/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.jdbc.boot.impl;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.jdbc.pool.impl.H2ReactiveConnectionPoolInitiator;
import org.hibernate.service.spi.ServiceContributor;

public class H2ServiceContributor implements ServiceContributor {
	@Override
	public void contribute(StandardServiceRegistryBuilder serviceRegistryBuilder) {
			serviceRegistryBuilder.addInitiator( H2ReactiveConnectionPoolInitiator.INSTANCE );
			serviceRegistryBuilder.addInitiator( JdbcSqlClientPoolConfigurationInitiator.INSTANCE );
	}
}

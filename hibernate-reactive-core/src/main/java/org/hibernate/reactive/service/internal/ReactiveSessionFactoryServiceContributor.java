/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.service.internal;

import org.hibernate.reactive.query.internal.ReactiveNativeQueryInterpreterInitiator;
import org.hibernate.service.spi.SessionFactoryServiceContributor;
import org.hibernate.service.spi.SessionFactoryServiceRegistryBuilder;

public class ReactiveSessionFactoryServiceContributor implements SessionFactoryServiceContributor {
	@Override
	public void contribute(SessionFactoryServiceRegistryBuilder serviceRegistryBuilder) {
		serviceRegistryBuilder.addInitiator( ReactiveNativeQueryInterpreterInitiator.INSTANCE );
	}
}

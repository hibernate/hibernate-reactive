/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.service;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.hql.spi.QueryTranslatorFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

public class ReactiveQueryTranslatorFactoryInitiator implements StandardServiceInitiator<QueryTranslatorFactory> {
	public static final ReactiveQueryTranslatorFactoryInitiator INSTANCE = new ReactiveQueryTranslatorFactoryInitiator();

	@Override
	public QueryTranslatorFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveQueryTranslatorFactory();
	}

	@Override
	public Class<QueryTranslatorFactory> getServiceInitiated() {
		return QueryTranslatorFactory.class;
	}
}

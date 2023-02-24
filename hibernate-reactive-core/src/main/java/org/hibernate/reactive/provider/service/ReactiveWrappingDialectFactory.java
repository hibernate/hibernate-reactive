/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.internal.DialectFactoryImpl;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfoSource;
import org.hibernate.reactive.dialect.ReactiveDialectWrapper;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveWrappingDialectFactory implements DialectFactory, ServiceRegistryAwareService {

	private final DialectFactoryImpl original = new DialectFactoryImpl();

	@Override
	public Dialect buildDialect(Map<String, Object> configValues, DialectResolutionInfoSource resolutionInfoSource)
			throws HibernateException {
		return wrap( original.buildDialect( configValues, resolutionInfoSource ) );
	}

	private Dialect wrap(final Dialect dialect) {
		if ( dialect == null ) {
			return null;
		}
		else {
			return new ReactiveDialectWrapper( dialect );
		}
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		original.injectServices( serviceRegistry );
	}

}

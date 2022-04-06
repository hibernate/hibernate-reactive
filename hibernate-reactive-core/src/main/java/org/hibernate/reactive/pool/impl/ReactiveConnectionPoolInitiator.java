/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.boot.registry.classloading.spi.ClassLoadingException;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceException;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our {@link ReactiveConnectionPool}. By default, the pool
 * implementation is {@link DefaultSqlClientPool}. A custom implementation may
 * be specified via {@link Settings#SQL_CLIENT_POOL}.
 *
 * @see ReactiveConnectionPool
 * @see DefaultSqlClientPool
 */
public class ReactiveConnectionPoolInitiator implements StandardServiceInitiator<ReactiveConnectionPool> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public static final ReactiveConnectionPoolInitiator INSTANCE = new ReactiveConnectionPoolInitiator();

	public ReactiveConnectionPoolInitiator() {}

	@Override
	public ReactiveConnectionPool initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		Object configValue = configurationValues.get( Settings.SQL_CLIENT_POOL );
		if ( configValue == null ) {
			return new DefaultSqlClientPool();
		}

		if ( configValue instanceof ReactiveConnectionPool ) {
			return (ReactiveConnectionPool) configValue;
		}
		else {
			final Class<ReactiveConnectionPool> implClass;
			if ( configValue instanceof Class ) {
				implClass = (Class) configValue;
			}
			else {
				final String className = configValue.toString();
				final ClassLoaderService classLoaderService = registry.getService( ClassLoaderService.class );
				try {
					implClass = classLoaderService.classForName( className );
				}
				catch (ClassLoadingException cle) {
					throw new ServiceException( "Unable to locate specified reactive connection pool [" + className + "]" );
				}
			}

			try {
				LOG.instantiatingReactivePool( implClass );
				return implClass.newInstance();
			}
			catch (Exception e) {
				throw new ServiceException( "Unable to instantiate specified reactive connection pool [" + implClass.getName() + "]" );
			}
		}
	}

	@Override
	public Class<ReactiveConnectionPool> getServiceInitiated() {
		return ReactiveConnectionPool.class;
	}
}

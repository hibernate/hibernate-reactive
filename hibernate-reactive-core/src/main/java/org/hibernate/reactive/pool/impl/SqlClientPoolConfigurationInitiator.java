/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.HibernateException;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * allows the user to define their own {@link SqlClientPoolConfiguration}
 * strategy.
 */
public class SqlClientPoolConfigurationInitiator implements StandardServiceInitiator<SqlClientPoolConfiguration> {

    private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

    public static final SqlClientPoolConfigurationInitiator INSTANCE = new SqlClientPoolConfigurationInitiator();

    @Override
    public SqlClientPoolConfiguration initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
        String configClassName = (String) configurationValues.get( Settings.SQL_CLIENT_POOL_CONFIG );
        if ( configClassName==null ) {
            return new DefaultSqlClientPoolConfiguration();
        }
        else {
            LOG.sqlClientConfiguration( configClassName );
            final ClassLoaderService classLoaderService = registry.getService( ClassLoaderService.class );
            try {
                return (SqlClientPoolConfiguration) classLoaderService.classForName( configClassName ).newInstance();
            }
            catch (Exception e) {
                throw new HibernateException(
                        "Could not instantiate SQL client pool configuration [" + configClassName + "]", e
                );
            }
        }
    }

    @Override
    public Class<SqlClientPoolConfiguration> getServiceInitiated() {
        return SqlClientPoolConfiguration.class;
    }
}

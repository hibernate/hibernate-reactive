/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.engine.jdbc.connections.spi.MultiTenantConnectionProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} for
 * {@link NoJdbcMultiTenantConnectionProvider}.
 *
 * @author Gavin King
 */
public class NoJdbcMultiTenantConnectionProviderInitiator implements StandardServiceInitiator<MultiTenantConnectionProvider>  {

    public static final NoJdbcMultiTenantConnectionProviderInitiator INSTANCE = new NoJdbcMultiTenantConnectionProviderInitiator();

    @Override
    public MultiTenantConnectionProvider initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
        //TODO surely we should interpret some meaning!?
        //Couldn't find any other way to force the SessionFactory into multi-tenancy mode. See https://hibernate.atlassian.net/browse/HHH-16246
        if ( !configurationValues.containsKey( AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER ) ) {
            //If this configuration key is not set, then multi-tenancy won't happen:
            return null;
        }
        //Otherwise, return a non-null implementation to ensure the SessionFactory enforces the use of a tenantId consistently
        //across all operations.
        return new NoJdbcMultiTenantConnectionProvider();
    }

    @Override
    public Class<MultiTenantConnectionProvider> getServiceInitiated() {
        return MultiTenantConnectionProvider.class;
    }
}

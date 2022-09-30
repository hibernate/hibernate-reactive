/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
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
//        final MultiTenancyStrategy strategy = MultiTenancyStrategy.determineMultiTenancyStrategy(  configurationValues );
//        if ( !strategy.requiresMultiTenantConnectionProvider() ) {
//            // nothing to do, but given the separate hierarchies have to handle this here.
//            return null;
//        }

//        return new NoJdbcMultiTenantConnectionProvider();
        return null;
    }

    @Override
    public Class<MultiTenantConnectionProvider> getServiceInitiated() {
        return MultiTenantConnectionProvider.class;
    }
}

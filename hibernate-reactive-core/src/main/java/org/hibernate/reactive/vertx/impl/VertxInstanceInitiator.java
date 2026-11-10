/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx.impl;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.vertx.VertxFactory;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * Factory for the default implementation of {@link VertxInstance}.
 */
public final class VertxInstanceInitiator implements StandardServiceInitiator<VertxInstance> {

    public static final VertxInstanceInitiator INSTANCE = new VertxInstanceInitiator();

    @Override
    public VertxInstance initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
        final Object setting = configurationValues.get( Settings.VERTX_FACTORY );
        if ( setting == null ) {
            return new DefaultVertxInstance();
        }
        // A VertxFactory, its class, or the name of its class
        final VertxFactory vertxFactory = registry.requireService( StrategySelector.class )
                .resolveStrategy( VertxFactory.class, setting );
        return new DefaultVertxInstance( vertxFactory );
    }

    @Override
    public Class<VertxInstance> getServiceInitiated() {
        return VertxInstance.class;
    }

}

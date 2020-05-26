/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx.impl;

import org.hibernate.boot.registry.StandardServiceInitiator;
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
        return new DefaultVertxInstance();
    }

    @Override
    public Class<VertxInstance> getServiceInitiated() {
        return VertxInstance.class;
    }

}

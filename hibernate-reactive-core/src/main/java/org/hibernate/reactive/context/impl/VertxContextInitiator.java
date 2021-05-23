/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.context.Context;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

public class VertxContextInitiator implements StandardServiceInitiator<Context> {

    public static final VertxContextInitiator INSTANCE = new VertxContextInitiator();

    @Override
    public Context initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
        return new VertxContext();
    }

    @Override
    public Class<Context> getServiceInitiated() {
        return Context.class;
    }
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import io.vertx.core.Vertx;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * An adaptor for the Vert.x {@link io.vertx.core.Context}.
 *
 * @author Gavin King
 */
public class VertxContext implements Context, ServiceRegistryAwareService {

    private VertxInstance vertxInstance;

    private io.vertx.core.Context getOrCreateContext() {
        return vertxInstance.getVertx().getOrCreateContext();
    }

    private io.vertx.core.Context currentContext() {
        return Vertx.currentContext();
    }

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        vertxInstance = serviceRegistry.getService( VertxInstance.class );
    }

    @Override
    public <T> void put(Class<T> key, String id, T instance) {
        getOrCreateContext().putLocal( id, instance );
    }

    @Override
    public <T> T get(Class<T> key, String id) {
        final io.vertx.core.Context context = currentContext();
        if ( context != null ) {
            return context.getLocal( id );
        }
        else {
            return null;
        }
    }

    @Override
    public void remove(Class<?> key, String id) {
        final io.vertx.core.Context context = currentContext();
        if ( context != null ) {
            context.removeLocal( id );
        }
    }

    @Override
    public void execute(Runnable runnable) {
        io.vertx.core.Context context = getOrCreateContext();
        if ( Vertx.currentContext() == context ) {
            runnable.run();
        }
        else {
            context.runOnContext( x -> runnable.run() );
        }
    }
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import java.lang.invoke.MethodHandles;

import io.vertx.core.Vertx;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * An adaptor for the Vert.x {@link io.vertx.core.Context}.
 *
 * @author Gavin King
 */
public class VertxContext implements Context, ServiceRegistryAwareService {

    private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

    private VertxInstance vertxInstance;

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        vertxInstance = serviceRegistry.getService( VertxInstance.class );
    }

    @Override
    public <T> void put(Key<T> key, T instance) {
        final io.vertx.core.Context context = Vertx.currentContext();
        if ( context != null ) {
            context.put( key, instance );
        }
        else {
            throw LOG.notVertxContextActive();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Key<T> key) {
        final io.vertx.core.Context context = Vertx.currentContext();
        if ( context != null ) {
            return (T) context.get( key );
        }
        else {
            return null;
        }
    }

    @Override
    public void remove(Key<?> key) {
        final io.vertx.core.Context context = Vertx.currentContext();
        if ( context != null ) {
            context.remove( key );
        }
    }

    @Override
    public void execute(Runnable runnable) {
        io.vertx.core.Context context = vertxInstance.getVertx().getOrCreateContext();
        if ( Vertx.currentContext() == context ) {
            runnable.run();
        }
        else {
            context.runOnContext( x -> runnable.run() );
        }
    }
}

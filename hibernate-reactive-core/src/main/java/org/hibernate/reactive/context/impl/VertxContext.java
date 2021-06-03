/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
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

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        vertxInstance = serviceRegistry.getService( VertxInstance.class );
    }

    static ContextInternal currentContext() {
        return (ContextInternal) Vertx.currentContext();
    }

    @Override
    public <T> void put(Key<T> key, T instance) {
        final ContextInternal context = currentContext();
        if ( context != null ) {
            context.localContextData().put( key, instance );
        }
        else {
            throw new IllegalStateException("No Vert.x context active");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Key<T> key) {
        final ContextInternal context = currentContext();
        if ( context != null ) {
            return (T) context.localContextData().get( key );
        }
        else {
            return null;
        }
    }

    @Override
    public void remove(Key<?> key) {
        final ContextInternal context = currentContext();
        if ( context != null ) {
            context.localContextData().remove( key );
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

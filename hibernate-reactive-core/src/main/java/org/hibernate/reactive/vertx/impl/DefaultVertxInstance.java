/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import io.vertx.core.Vertx;

import org.hibernate.AssertionFailure;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

/**
 * A singleton instance of {@link Vertx} that is created on
 * demand and destroyed automatically along with the Hibernate
 * {@link org.hibernate.SessionFactory#close() session factory}.
 *
 * @see ProvidedVertxInstance if you need to a different instance
 *
 * @author Sanne Grinovero <sanne@hibernate.org>
 */
public final class DefaultVertxInstance implements VertxInstance, Stoppable, Startable {

    private final static Method CLOSE_METHOD = identifyCloseMethod();

    private static Method identifyCloseMethod() {
        try {
            return Vertx.class.getMethod( "close" );
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException( e );
        }
    }

    private Vertx vertx;

    @Override
    public Vertx getVertx() {
        if ( vertx == null ) {
            throw new IllegalStateException("Service not initialized");
        }
        return vertx;
    }

    @Override
    public void stop() {
        if ( vertx != null ) {
            try {
                CLOSE_METHOD.invoke( vertx );
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new AssertionFailure( "Error closing Vert.x instance", e );
            }
        }
    }

    @Override
    public void start() {
        vertx = Vertx.vertx();
    }

}

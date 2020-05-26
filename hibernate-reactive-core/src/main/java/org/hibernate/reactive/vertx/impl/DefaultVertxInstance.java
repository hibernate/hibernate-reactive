/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx.impl;

import io.vertx.core.Vertx;
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
            vertx.close();
        }
    }

    @Override
    public void start() {
        vertx = Vertx.vertx();
    }

}

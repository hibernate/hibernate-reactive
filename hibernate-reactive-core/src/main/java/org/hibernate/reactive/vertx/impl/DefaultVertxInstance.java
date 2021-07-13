/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx.impl;

import java.lang.invoke.MethodHandles;

import io.vertx.core.Vertx;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
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

    private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

    private Vertx vertx;

    @Override
    public Vertx getVertx() {
        if ( vertx == null ) {
            throw LOG.serviceNotInitialized();
        }
        return vertx;
    }

    @Override
    public void stop() {
        if ( vertx != null ) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Override
    public void start() {
        vertx = Vertx.vertx();
    }

}

package org.hibernate.reactive.service;

import io.vertx.core.Vertx;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

/**
 * This will start a new Vertx instance on demand, then ensure it's closed when
 * the SessionFactory is closed.
 * To use an external Vertx instance you can inject an alternative implementation
 * in the bootstrap registry.
 *
 * @author Sanne Grinovero <sanne@hibernate.org>
 */
public final class SelfmanagingVertxService implements VertxService, Stoppable, Startable {

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

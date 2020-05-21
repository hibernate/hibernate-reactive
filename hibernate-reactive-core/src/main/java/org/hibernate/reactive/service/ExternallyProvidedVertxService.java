package org.hibernate.reactive.service;

import io.vertx.core.Vertx;

import java.util.Objects;

/**
 * If the framework / container / runtime already has a Vert.x instance which
 * it wants to reuse, use this VertxService implementation to inject it into
 * Hibernate Reactive registry at boot time.
 * When using this service, Hibernate will not reconfigure nor stop the
 * Vert.x instance on shutdown as this responsibility belongs to the
 * code managing it.
 */
public final class ExternallyProvidedVertxService implements VertxService {

    private final Vertx vertx;

    public ExternallyProvidedVertxService(Vertx vertx) {
        Objects.requireNonNull( vertx );
        this.vertx = vertx;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}

package org.hibernate.reactive.service;

import io.vertx.core.Vertx;

import java.util.Objects;

public final class ManagedVertxService implements VertxService {

    private final Vertx vertx;

    public ManagedVertxService(Vertx vertx) {
        Objects.requireNonNull( vertx );
        this.vertx = vertx;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}

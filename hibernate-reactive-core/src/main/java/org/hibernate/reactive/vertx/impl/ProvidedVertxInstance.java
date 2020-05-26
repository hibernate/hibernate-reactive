/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx.impl;

import io.vertx.core.Vertx;
import org.hibernate.reactive.vertx.VertxInstance;

import java.util.Objects;

/**
 * An implementation of {@link VertxInstance} which allows the client
 * to provide an instance of {@link Vertx} whose lifecyle is managed
 * externally to Hibernate Reactive. The {@code ProvidedVertxInstance}
 * must be registered with explicitly Hibernate by calling
 * {@link org.hibernate.boot.registry.StandardServiceRegistryBuilder#addService}.
 * Hibernate will destroy the {@code Vertx} instance on shutdown.
 *
 * <pre>
 * new StandardServiceRegistryBuilder()
 *     .applySettings( properties )
 *     .addService( VertxInstance.class, new ProvidedVertxInstance( vertx ) )
 *     .build();
 * </pre>
 */
public final class ProvidedVertxInstance implements VertxInstance {

    private final Vertx vertx;

    public ProvidedVertxInstance(Vertx vertx) {
        Objects.requireNonNull( vertx );
        this.vertx = vertx;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}

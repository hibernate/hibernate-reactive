/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx;

import io.vertx.core.Vertx;

import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.service.Service;

/**
 * Used by {@link DefaultSqlClientPool} and
 * {@link org.hibernate.reactive.context.impl.VertxContext}
 * to obtain an instance of {@link Vertx}.
 * <p>
 * The default implementation is
 * {@link org.hibernate.reactive.vertx.impl.DefaultVertxInstance}.
 * When Hibernate Reactive starts, it reuses the instance of {@code Vertx}
 * that owns the current Vert.x context, if there is one, or otherwise
 * creates a new instance of {@code Vertx}, which is closed along with
 * the session factory. In a program which manages its own instance of
 * {@code Vertx}, this may result in multiple instances of {@code Vertx},
 * and so such programs should register a
 * {@link org.hibernate.reactive.vertx.impl.ProvidedVertxInstance}
 * or a custom {@code VertxInstance}.
 * <p>
 * A program may integrate a custom {@link VertxInstance}
 * with Hibernate Reactive by contributing a new service using a
 * {@link org.hibernate.boot.registry.StandardServiceInitiator}
 * or from code-based Hibernate configuration by calling
 * {@link org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder#addService}.
 *
 * <pre>{@code
 * new ReactiveServiceRegistryBuilder()
 *     .applySettings( properties )
 *     .addService( VertxInstance.class, (VertxInstance) () -> myVertx )
 *     .build();
 * }</pre>
 *
 * @see org.hibernate.reactive.vertx.impl.ProvidedVertxInstance
 */
@FunctionalInterface
public interface VertxInstance extends Service {

    /**
     * Obtain the instance of {@link Vertx}.
     */
    Vertx getVertx();

}

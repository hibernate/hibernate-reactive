/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx;

import io.vertx.core.Vertx;

import org.hibernate.service.Service;

/**
 * Used by {@link org.hibernate.reactive.pool.impl.SqlClientPool}
 * to obtain an instance of {@link Vertx}. The default instance is
 * {@link org.hibernate.reactive.vertx.impl.DefaultVertxInstance}.
 * <p>
 * A program may integrate a custom {@link VertxInstance}
 * with Hibernate Reactive by contributing a new service using a
 * {@link org.hibernate.boot.registry.StandardServiceInitiator}
 * or from code-based Hibernate configuration by calling
 * {@link org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder#addService}.
 *
 * <pre>
 * new ReactiveServiceRegistryBuilder()
 *     .applySettings( properties )
 *     .addService( VertxInstance.class, () -> myVertx )
 *     .build();
 * </pre>
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

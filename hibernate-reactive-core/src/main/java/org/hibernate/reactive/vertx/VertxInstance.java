/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx;

import io.vertx.core.Vertx;

import org.hibernate.service.Service;

/**
 * Used by {@link org.hibernate.reactive.pool.impl.DefaultSqlClientPool}
 * and {@link org.hibernate.reactive.context.impl.VertxContext} to
 * obtain an instance of {@link Vertx}.
 * <p>
 * The default implementation is
 * {@link org.hibernate.reactive.vertx.impl.DefaultVertxInstance},
 * which creates a new instance of {@code Vertx} if there is no
 * instance already associated with the calling thread. This default
 * behavior may cause problems in programs which have an instance of
 * {@code Vertx} whose lifecycle is managed externally to Hibernate
 * Reactive, and in such cases
 * {@link org.hibernate.reactive.vertx.impl.ProvidedVertxInstance}
 * or a custom-written {@code VertxInstance} should be used.
 * <p>
 * A program may integrate a custom {@link VertxInstance} with
 * Hibernate Reactive by contributing a new service using a
 * {@link org.hibernate.boot.registry.StandardServiceInitiator} or
 * from code-based Hibernate configuration by calling the method
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

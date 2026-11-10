/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.vertx;

import io.vertx.core.Vertx;

import org.hibernate.Incubating;

/**
 * Creates the instance of {@link Vertx} that Hibernate Reactive uses when it
 * doesn't start in a Vert.x context, for example to create it with other options:
 * <pre>
 * public class MyVertxFactory implements VertxFactory {
 *     &#64;Override
 *     public Vertx createVertx() {
 *         return Vertx.vertx( new VertxOptions().setEventLoopPoolSize( 4 ) );
 *     }
 * }
 * </pre>
 * Name it with the setting {@value org.hibernate.reactive.provider.Settings#VERTX_FACTORY}.
 * Hibernate Reactive closes the instance it creates when the session factory is closed.
 * When it starts in a Vert.x context, it uses the instance of {@link Vertx} owning the context instead,
 * and doesn't call the factory.
 * <p>
 * It's only used by the default {@link VertxInstance}.
 */
@Incubating(since = "5.0")
@FunctionalInterface
public interface VertxFactory {

	/**
	 * Create the instance of {@link Vertx}.
	 */
	Vertx createVertx();
}

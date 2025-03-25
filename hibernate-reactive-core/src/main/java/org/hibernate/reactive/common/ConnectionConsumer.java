/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import org.hibernate.Incubating;

import java.util.concurrent.CompletionStage;

/**
 * An operation which makes direct use of a database connection.
 *
 * @param <C> the connection type, usually
 *            {@link io.vertx.sqlclient.SqlConnection}
 * @param <R> the result type of the operation, or {@link Void}
 *
 * @author Gavin King
 */
@Incubating @FunctionalInterface
public interface ConnectionConsumer<C, R> {
    CompletionStage<R> accept(C connection);
}

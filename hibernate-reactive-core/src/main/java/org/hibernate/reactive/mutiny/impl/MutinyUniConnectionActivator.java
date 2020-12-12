/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.reactive.pool.ReactiveConnection;

import java.util.concurrent.CompletionStage;

/**
 * Internal helper interface to "enhance" the {@link Uni}s returned by Mutiny methods
 * to acquire the database connection on subscription.
 * <p>
 * It's implemented as a functional-interface with a static create methods so that the
 * implementing classes can pass a method-reference e.g. from {@link MutinySessionImpl}
 * to {@link MutinyQueryImpl} without having to pass around even more delegates.
 */
@FunctionalInterface
interface MutinyUniConnectionActivator {
    <R> Uni<R> asUni(CompletionStage<R> completionStage);

    /**
     * Creates an instance of {@link MutinyUniConnectionActivator} that hooks into the
     * subscription phase of an {@link Uni} created for a {@link CompletionStage} using
     * the given {@link ReactiveConnection}.
     */
    static MutinyUniConnectionActivator create(ReactiveConnection reactiveConnection) {
        return new MutinyUniConnectionActivator() {
            @Override
            public <R> Uni<R> asUni(CompletionStage<R> completionStage) {
                return Uni.createFrom().completionStage( completionStage )
                        .onSubscribe()
                        .invoke(
                                // Just need to trigger the async process to acquire a
                                // connection in this "onSubscribe" callback, no need
                                // to return a new Uni or so.
                                reactiveConnection::openConnection
                        );
            }
        };
    }
}

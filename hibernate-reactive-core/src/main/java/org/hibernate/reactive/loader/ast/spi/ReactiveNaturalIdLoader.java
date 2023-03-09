/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.NaturalIdLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

public interface ReactiveNaturalIdLoader<T> extends NaturalIdLoader<CompletionStage<T>> {

    Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

    /**
     * @deprecated use {@link #reactiveResolveNaturalIdToId(Object, SharedSessionContractImplementor)}
     */
    @Deprecated
    @Override
    default Object resolveNaturalIdToId(Object naturalIdValue, SharedSessionContractImplementor session) {
        throw LOG.nonReactiveMethodCall("reactiveResolveNaturalIdToId");
    }

    CompletionStage<Object> reactiveResolveNaturalIdToId(Object naturalIdValue, SharedSessionContractImplementor session);

    /**
     * @deprecated use {@link #reactiveResolveIdToNaturalId(Object, SharedSessionContractImplementor)}
     */
    default Object resolveIdToNaturalId(Object id, SharedSessionContractImplementor session) {
        throw LOG.nonReactiveMethodCall("reactiveResolveIdToNaturalId");
    }

    CompletionStage<Object> reactiveResolveIdToNaturalId(Object id, SharedSessionContractImplementor session);
}

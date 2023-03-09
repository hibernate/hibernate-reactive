/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.MultiNaturalIdLoadOptions;
import org.hibernate.loader.ast.spi.MultiNaturalIdLoader;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface ReactiveMultiNaturalIdLoader<T> extends MultiNaturalIdLoader<CompletionStage<T>> {
    /**
     * Load multiple entities by natural-id.  The exact result depends on the passed options.
     *
     * @param naturalIds The natural-ids to load.  The values of this array will depend on whether the
     *                   natural-id is simple or complex.
     * @param options
     * @param session
     */
    @Override
    <K> List<CompletionStage<T>> multiLoad(K[] naturalIds, MultiNaturalIdLoadOptions options, SharedSessionContractImplementor session);
}

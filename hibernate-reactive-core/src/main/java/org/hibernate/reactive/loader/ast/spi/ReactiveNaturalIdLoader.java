/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.NaturalIdLoader;

public interface ReactiveNaturalIdLoader<T> extends NaturalIdLoader<InternalStage<T>> {

    @Override
    InternalStage<Object> resolveNaturalIdToId(Object naturalIdValue, SharedSessionContractImplementor session);

    @Override
    InternalStage<Object> resolveIdToNaturalId(Object id, SharedSessionContractImplementor session);
}

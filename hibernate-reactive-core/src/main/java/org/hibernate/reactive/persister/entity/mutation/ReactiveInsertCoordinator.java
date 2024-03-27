/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;

/**
 * @see org.hibernate.persister.entity.mutation.InsertCoordinator
 */
public interface ReactiveInsertCoordinator {

	CompletionStage<GeneratedValues> reactiveInsert(Object entity, Object[] values, SharedSessionContractImplementor session);

	CompletionStage<GeneratedValues> reactiveInsert(Object entity, Object id, Object[] values, SharedSessionContractImplementor session);

}

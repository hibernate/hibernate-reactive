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
 * Scoped to a single operation, so that we can keep
 * instance scoped state.
 *
 * @see org.hibernate.persister.entity.mutation.UpdateCoordinator
 * @see ReactiveUpdateCoordinator
 */
public interface ReactiveScopedUpdateCoordinator {

	CompletionStage<GeneratedValues> reactiveUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object oldVersion,
			Object[] incomingOldValues,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			SharedSessionContractImplementor session);

}

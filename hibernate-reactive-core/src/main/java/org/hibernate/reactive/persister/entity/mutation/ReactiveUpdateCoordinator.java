/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.UpdateCoordinator;

public interface ReactiveUpdateCoordinator extends UpdateCoordinator {

	CompletionStage<Void> coordinateReactiveUpdate(
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

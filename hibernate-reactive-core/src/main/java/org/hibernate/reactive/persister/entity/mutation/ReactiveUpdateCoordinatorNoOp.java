/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.UpdateCoordinatorNoOp;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveUpdateCoordinatorNoOp extends UpdateCoordinatorNoOp implements ReactiveScopedUpdateCoordinator, ReactiveUpdateCoordinator {

	public ReactiveUpdateCoordinatorNoOp(AbstractEntityPersister entityPersister) {
		super( entityPersister );
	}

	@Override
	public void coordinateUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object oldVersion,
			Object[] incomingOldValues,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			SharedSessionContractImplementor session) {
	}

	@Override
	public InternalStage<Void> coordinateReactiveUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object oldVersion,
			Object[] incomingOldValues,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			SharedSessionContractImplementor session) {
		return voidFuture();
	}

	@Override
	public ReactiveScopedUpdateCoordinator makeScopedCoordinator() {
		//This particular implementation is stateless, so we can return ourselves w/o needing to create a scope.
		return this;
	}

}

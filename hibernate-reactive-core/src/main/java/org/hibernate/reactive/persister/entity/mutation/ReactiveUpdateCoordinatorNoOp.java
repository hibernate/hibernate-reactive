/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.UpdateCoordinatorNoOp;

import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;

public class ReactiveUpdateCoordinatorNoOp extends UpdateCoordinatorNoOp implements ReactiveScopedUpdateCoordinator, ReactiveUpdateCoordinator {

	public ReactiveUpdateCoordinatorNoOp(EntityPersister entityPersister) {
		super( entityPersister );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object oldVersion,
			Object[] incomingOldValues,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			SharedSessionContractImplementor session) {
		return nullFuture();
	}

	@Override
	public ReactiveScopedUpdateCoordinator makeScopedCoordinator() {
		//This particular implementation is stateless, so we can return ourselves w/o needing to create a scope.
		return this;
	}

}

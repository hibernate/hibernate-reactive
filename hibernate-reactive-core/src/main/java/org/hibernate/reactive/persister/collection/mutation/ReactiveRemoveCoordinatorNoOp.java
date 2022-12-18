/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorNoOp;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveRemoveCoordinatorNoOp extends RemoveCoordinatorNoOp implements ReactiveRemoveCoordinator {

	public ReactiveRemoveCoordinatorNoOp(CollectionMutationTarget mutationTarget) {
		super( mutationTarget );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteAllRows(Object key, SharedSessionContractImplementor session) {
		return voidFuture();
	}

	// TODO: Update ORM and inherit this
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + getMutationTarget().getRolePath() + " [DISABLED])";
	}
}

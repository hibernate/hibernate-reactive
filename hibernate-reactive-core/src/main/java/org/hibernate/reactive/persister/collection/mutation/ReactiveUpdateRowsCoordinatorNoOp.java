/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinatorNoOp;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.persister.collection.mutation.UpdateRowsCoordinatorNoOp
 */
public class ReactiveUpdateRowsCoordinatorNoOp extends UpdateRowsCoordinatorNoOp implements ReactiveUpdateRowsCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveUpdateRowsCoordinatorNoOp(CollectionMutationTarget mutationTarget) {
		super( mutationTarget );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateRows(Object key, PersistentCollection<?> collection, SharedSessionContractImplementor session) {
		return voidFuture();
	}

	@Override
	public void updateRows(Object key, PersistentCollection<?> collection, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveUpdateRows" );
	}

	@Override
	public String toString() {
		return "UpdateRowsCoordinator(" + getMutationTarget().getRolePath() + " (no-op))";
	}

}

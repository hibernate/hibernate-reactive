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
import org.hibernate.persister.collection.mutation.InsertRowsCoordinatorNoOp;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * {@link InsertRowsCoordinatorNoOp}
 */
public class ReactiveInsertRowsCoordinatorNoOp implements ReactiveInsertRowsCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );
	private final CollectionMutationTarget mutationTarget;

	public ReactiveInsertRowsCoordinatorNoOp(CollectionMutationTarget mutationTarget) {
		this.mutationTarget = mutationTarget;
	}

	@Override
	public String toString() {
		return "ReactiveInsertRowsCoordinator(" + mutationTarget.getRolePath() + " (no-op))";
	}


	@Override
	public void insertRows(PersistentCollection<?> collection, Object id, EntryFilter entryChecker, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveInsertRows" );
	}

	@Override
	public CompletionStage<Void> reactiveInsertRows(PersistentCollection<?> collection, Object id, EntryFilter entryChecker, SharedSessionContractImplementor session) {
		return voidFuture();
	}

	@Override
	public CollectionMutationTarget getMutationTarget() {
		return mutationTarget;
	}
}

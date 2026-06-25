/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.action.queue.spi.decompose.collection.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinatorNoOp;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;

import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

public class ReactiveDeleteRowsCoordinatorNoOp extends DeleteRowsCoordinatorNoOp implements ReactiveDeleteRowsCoordinator {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveDeleteRowsCoordinatorNoOp(CollectionMutationTarget mutationTarget) {
		super( mutationTarget );
	}

	@Override
	public void deleteRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveDeleteRows" );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		return voidFuture();
	}
}

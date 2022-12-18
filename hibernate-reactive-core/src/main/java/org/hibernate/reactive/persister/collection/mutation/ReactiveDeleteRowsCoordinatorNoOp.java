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
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinatorNoOp;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

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

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import java.lang.invoke.MethodHandles;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.InsertRowsCoordinatorStandard;
import org.hibernate.persister.collection.mutation.RowMutationOperations;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

public class ReactiveInsertRowsCoordinatorStandard extends InsertRowsCoordinatorStandard {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveInsertRowsCoordinatorStandard(CollectionMutationTarget mutationTarget, RowMutationOperations rowMutationOperations) {
		super( mutationTarget, rowMutationOperations );
	}

	@Override
	public void insertRows(PersistentCollection<?> collection, Object id, EntryFilter entryChecker, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveInsertRows" );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.InsertRowsCoordinatorStandard;
import org.hibernate.persister.collection.mutation.RowMutationOperations;

public class ReactiveInsertRowsCoordinatorStandard extends InsertRowsCoordinatorStandard {

	public ReactiveInsertRowsCoordinatorStandard(CollectionMutationTarget mutationTarget, RowMutationOperations rowMutationOperations) {
		super( mutationTarget, rowMutationOperations );
	}
}

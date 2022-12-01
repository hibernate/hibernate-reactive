/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import org.hibernate.persister.collection.mutation.CollectionMutationTarget;
import org.hibernate.persister.collection.mutation.InsertRowsCoordinatorNoOp;

public class ReactiveInsertRowCoordinatorNoOp extends InsertRowsCoordinatorNoOp {

	public ReactiveInsertRowCoordinatorNoOp(CollectionMutationTarget mutationTarget) {
		super( mutationTarget );
	}
}

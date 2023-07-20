/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinator;

public interface ReactiveDeleteRowsCoordinator extends DeleteRowsCoordinator {

	InternalStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session);
}

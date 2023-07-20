/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.mutation;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinator;


public interface ReactiveUpdateRowsCoordinator extends UpdateRowsCoordinator {

	InternalStage<Void> reactiveUpdateRows(Object key, PersistentCollection<?> collection, SharedSessionContractImplementor session);

}

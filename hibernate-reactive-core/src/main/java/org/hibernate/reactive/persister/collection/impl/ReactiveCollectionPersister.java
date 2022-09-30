/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;

/**
 * A reactive {@link CollectionPersister}
 */
public interface ReactiveCollectionPersister extends CollectionPersister {

	/**
	 * Reactive version of {@link CollectionPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> recreateReactive(PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#remove(Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> removeReactive(Object id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#deleteRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveDeleteRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#insertRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInsertRows( PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of  {@link CollectionPersister#updateRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveUpdateRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#initialize(Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInitialize(Object key, SharedSessionContractImplementor session);
}

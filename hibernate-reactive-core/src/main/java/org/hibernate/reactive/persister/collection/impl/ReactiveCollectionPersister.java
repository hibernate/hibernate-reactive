/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link CollectionPersister}
 */
public interface ReactiveCollectionPersister extends CollectionPersister {

	/**
	 * Reactive version of {@link CollectionPersister#recreate(PersistentCollection, Serializable, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> recreateReactive(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#remove(Serializable, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> removeReactive(Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#deleteRows(PersistentCollection, Serializable, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveDeleteRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#insertRows(PersistentCollection, Serializable, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInsertRows( PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of  {@link CollectionPersister#updateRows(PersistentCollection, Serializable, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveUpdateRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link CollectionPersister#initialize(Serializable, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInitialize(Serializable key, SharedSessionContractImplementor session);
}

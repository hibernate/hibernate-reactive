/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.AbstractCollectionPersister;

/**
 * Similar to {@link AbstractCollectionPersister} in ORM
 * <p>
 *     The reactive classes already extend something so we cannot have an abstract class like in ORM.
 * </p>
 */
public interface ReactiveCollectionPersister {

	/**
	 * Reactive version of {@link AbstractCollectionPersister#recreate(PersistentCollection, Serializable , SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> recreateReactive(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link AbstractCollectionPersister#remove(Serializable , SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> removeReactive(Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link AbstractCollectionPersister#deleteRows(PersistentCollection, Serializable , SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveDeleteRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link AbstractCollectionPersister#insertRows(PersistentCollection, Serializable , SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInsertRows( PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of  {@link AbstractCollectionPersister#updateRows(PersistentCollection, Serializable , SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveUpdateRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);
}

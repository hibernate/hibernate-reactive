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

/**
 * Similar to {@link org.hibernate.persister.collection.AbstractCollectionPersister} in ORM
 * <p>
 *     The reactive classes already extend something so we cannot have an abstract class like in ORM.
 * </p>
 */
public interface ReactiveCollectionPersister {

	/**
	 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}.recreate()
	 * @param collection
	 * @param id
	 * @param session
	 * @return Void completion stage
	 */
	CompletionStage<Void> recreateReactive(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}.remove()
	 * @param id
	 * @param session
	 * @return Integer completion stage
	 */
	CompletionStage<Integer> removeReactive(Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}.deleteRows()
	 * @param collection
	 * @param id
	 * @param session
	 * @return Void completion stage
	 */
	CompletionStage<Void> reactiveDeleteRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}.insertRows()
	 * @param collection
	 * @param id
	 * @param session
	 * @return Void completion stage
	 */
	CompletionStage<Void> reactiveInsertRows( PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);

	/**
	 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}.updateRows()
	 * @param collection
	 * @param id
	 * @param session
	 * @return Void completion stage
	 */
	CompletionStage<Void> reactiveUpdateRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session);
}

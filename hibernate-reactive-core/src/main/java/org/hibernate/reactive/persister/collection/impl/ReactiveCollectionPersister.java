/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

/**
 * A reactive {@link CollectionPersister}
 */
public interface ReactiveCollectionPersister extends CollectionPersister {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * Reactive version of {@link CollectionPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> recreateReactive(PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void remove(Object id, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveRemove" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#remove(Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> removeReactive(Object id, SharedSessionContractImplementor session);

	@Override
	default void deleteRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveDeleteRows" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#deleteRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveDeleteRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void insertRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveInsertRows" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#insertRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInsertRows( PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void updateRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveUpdateRows" );
	}

	/**
	 * Reactive version of  {@link CollectionPersister#updateRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveUpdateRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void initialize(Object key, SharedSessionContractImplementor session) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveInitialize" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#initialize(Object, SharedSessionContractImplementor)}
	 */
	CompletionStage<Void> reactiveInitialize(Object key, SharedSessionContractImplementor session);
}

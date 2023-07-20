/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.lang.invoke.MethodHandles;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.logging.impl.Log;

import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * A reactive {@link CollectionPersister}
 */
public interface ReactiveCollectionPersister extends CollectionPersister {

	/**
	 * Reactive version of {@link CollectionPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	InternalStage<Void> reactiveRecreate(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void remove(Object id, SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "reactiveRemove" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#remove(Object, SharedSessionContractImplementor)}
	 */
	InternalStage<Void> reactiveRemove(Object id, SharedSessionContractImplementor session);

	@Override
	default void deleteRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "reactiveDeleteRows" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#deleteRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	InternalStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void insertRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "reactiveInsertRows" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#insertRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	InternalStage<Void> reactiveInsertRows( PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void updateRows(PersistentCollection<?> collection, Object key, SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "reactiveUpdateRows" );
	}

	/**
	 * Reactive version of  {@link CollectionPersister#updateRows(PersistentCollection, Object, SharedSessionContractImplementor)}
	 */
	InternalStage<Void> reactiveUpdateRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

	@Override
	default void initialize(Object key, SharedSessionContractImplementor session) throws HibernateException {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "reactiveInitialize" );
	}

	/**
	 * Reactive version of {@link CollectionPersister#initialize(Object, SharedSessionContractImplementor)}
	 */
	InternalStage<Void> reactiveInitialize(Object key, SharedSessionContractImplementor session);
}

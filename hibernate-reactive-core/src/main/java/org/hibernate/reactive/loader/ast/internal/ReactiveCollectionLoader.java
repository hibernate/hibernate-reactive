/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.reactive.logging.impl.Log;

import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public interface ReactiveCollectionLoader extends CollectionLoader {

	@Override
	PluralAttributeMapping getLoadable();

	@Override
	default PersistentCollection<?> load(Object key, SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "reactiveLoad(Object, SharedSessionContractImplementor)" );
	}

	/**
	 * Load a collection by its key (not necessarily the same as its owner's PK).
	 */
	CompletionStage<PersistentCollection<?>> reactiveLoad(Object key, SharedSessionContractImplementor session);
}

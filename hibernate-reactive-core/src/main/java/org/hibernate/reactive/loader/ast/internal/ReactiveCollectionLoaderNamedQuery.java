/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;

/**
 * @see org.hibernate.loader.ast.internal.CollectionLoaderNamedQuery
 */
public class ReactiveCollectionLoaderNamedQuery implements ReactiveCollectionLoader {

	@Override
	public PluralAttributeMapping getLoadable() {
		throw new UnsupportedOperationException();
	}

	@Override
	public InternalStage<PersistentCollection<?>> reactiveLoad(Object key, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException();
	}
}

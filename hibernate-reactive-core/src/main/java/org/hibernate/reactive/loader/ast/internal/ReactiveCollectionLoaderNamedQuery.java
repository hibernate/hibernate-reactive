/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.query.named.NamedQueryMemento;

/**
 * @see org.hibernate.loader.ast.internal.CollectionLoaderNamedQuery
 */
public class ReactiveCollectionLoaderNamedQuery implements ReactiveCollectionLoader {

	private final CollectionPersister persister;
	private final NamedQueryMemento namedQueryMemento;

	public ReactiveCollectionLoaderNamedQuery(CollectionPersister persister, NamedQueryMemento namedQueryMemento) {
		this.persister = persister;
		this.namedQueryMemento = namedQueryMemento;
	}

	@Override
	public PluralAttributeMapping getLoadable() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<PersistentCollection<?>> reactiveLoad(Object key, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException();
	}
}

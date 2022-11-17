/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;

public class ReactiveCollectionLoaderSubSelectFetch implements ReactiveCollectionLoader {

	@Override
	public PluralAttributeMapping getLoadable() {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public CompletionStage<PersistentCollection<?>> reactiveLoad(Object key, SharedSessionContractImplementor session) {
		throw new NotYetImplementedFor6Exception();
	}
}

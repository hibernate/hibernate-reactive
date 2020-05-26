/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.collection.CollectionInitializer;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public interface ReactiveCollectionInitializer extends CollectionInitializer {
	CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session);
}

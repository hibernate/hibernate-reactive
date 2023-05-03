/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import org.hibernate.loader.ast.spi.BatchLoader;
import org.hibernate.loader.ast.spi.CollectionBatchLoader;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoader;

/**
 * @see org.hibernate.loader.ast.spi.CollectionBatchLoader
 */
public interface ReactiveCollectionBatchLoader extends CollectionBatchLoader, BatchLoader, ReactiveCollectionLoader {
}

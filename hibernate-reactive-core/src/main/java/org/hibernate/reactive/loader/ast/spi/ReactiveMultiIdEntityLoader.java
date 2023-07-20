/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import java.util.List;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.event.spi.EventSource;
import org.hibernate.loader.ast.spi.EntityMultiLoader;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;

/**
 * @see org.hibernate.loader.ast.spi.MultiIdEntityLoader
 */
public interface ReactiveMultiIdEntityLoader<T> extends EntityMultiLoader<InternalStage<T>> {

	<K> InternalStage<List<T>> load(K[] ids, MultiIdLoadOptions options, EventSource session);
}

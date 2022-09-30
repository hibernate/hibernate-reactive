/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.SingleEntityLoader;

/**
 * Reactive loader subtype for loading an entity by a single unique-key value.
 * @see org.hibernate.loader.ast.spi.SingleUniqueKeyEntityLoader
 */
public interface ReactiveSingleUniqueKeyEntityLoader<T> extends SingleEntityLoader<CompletionStage<T>> {

	/**
	 * Resolve the matching id
	 */
	Object resolveId(Object key, SharedSessionContractImplementor session);
}

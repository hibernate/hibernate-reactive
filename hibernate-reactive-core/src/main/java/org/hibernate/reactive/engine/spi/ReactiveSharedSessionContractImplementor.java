/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.spi;

import java.util.Set;
import java.util.concurrent.CompletionStage;


import org.hibernate.engine.spi.PersistenceContext;

import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;

/**
 * @see org.hibernate.engine.spi.SharedSessionContractImplementor
 */
public interface ReactiveSharedSessionContractImplementor {

	default CompletionStage<Boolean> reactiveAutoFlushIfRequired(Set<String> querySpaces) {
		return falseFuture();
	}

	PersistenceContext getPersistenceContext();
}

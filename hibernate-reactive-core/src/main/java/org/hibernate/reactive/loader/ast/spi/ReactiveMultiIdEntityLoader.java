/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.loader.ast.spi.MultiLoader;

public interface ReactiveMultiIdEntityLoader<T> extends MultiLoader<CompletionStage<T>> {

	<K> CompletionStage<List<T>> load(K[] ids, MultiIdLoadOptions options, SharedSessionContractImplementor session);
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.sql.spi.NativeQueryImplementor;

public interface ReactiveNativeQueryImplementor<R> extends NativeQueryImplementor<R> {

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<Optional<R>> reactiveUniqueResultOptional();

	CompletionStage<R> reactiveUniqueResult();

	CompletionStage<List<R>> reactiveList();

}

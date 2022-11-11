/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.query.hql.spi.SqmQueryImplementor;


/**
 * An internal contract between the reactive session implementation
 * and the {@link org.hibernate.reactive.stage.Stage.Query} and
 * {@link org.hibernate.reactive.mutiny.Mutiny.Query} APIs.
 *
 * @see ReactiveSession
 */
@Incubating
public interface ReactiveQuery<R> extends SqmQueryImplementor<R> {

//	void setParameterMetadata(InterpretedParameterMetadata parameterMetadata);

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<List<R>> getReactiveResultList();

	CompletionStage<R> getReactiveSingleResultOrNull();

	CompletionStage<Integer> executeReactiveUpdate();
}

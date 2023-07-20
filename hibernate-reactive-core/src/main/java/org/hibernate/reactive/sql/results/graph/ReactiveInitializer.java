/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.Incubating;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;

/**
 * @see org.hibernate.sql.results.graph.Initializer
 */
@Incubating
public interface ReactiveInitializer {

	InternalStage<Void> reactiveResolveInstance(ReactiveRowProcessingState rowProcessingState);

	InternalStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState);

}

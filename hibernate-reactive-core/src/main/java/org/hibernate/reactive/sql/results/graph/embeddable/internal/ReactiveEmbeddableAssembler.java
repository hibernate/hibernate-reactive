/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;

import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveDomainResultsAssembler;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.embeddable.EmbeddableInitializer;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableAssembler;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;

import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * @see org.hibernate.sql.results.graph.embeddable.internal.EmbeddableAssembler
 */
public class ReactiveEmbeddableAssembler extends EmbeddableAssembler implements ReactiveDomainResultsAssembler {

	public ReactiveEmbeddableAssembler(EmbeddableInitializer<?> initializer) {
		super( initializer );
	}

	@Override
	public CompletionStage<Object> reactiveAssemble(ReactiveRowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		final ReactiveInitializer<InitializerData> reactiveInitializer = (ReactiveInitializer<InitializerData>) getInitializer();
		final InitializerData data = reactiveInitializer.getData( rowProcessingState );
		final Initializer.State state = data.getState();
		if ( state == Initializer.State.KEY_RESOLVED ) {
			return reactiveInitializer
					.reactiveResolveInstance( data )
					.thenApply( v -> reactiveInitializer.getResolvedInstance( data ) );
		}
		return completedFuture( reactiveInitializer.getResolvedInstance( data ) );
	}
}

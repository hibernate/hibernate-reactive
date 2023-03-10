/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

@Incubating
public interface ReactiveDomainResultsAssembler<J> extends DomainResultAssembler<J> {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default J assemble(RowProcessingState rowProcessingState) {
		throw LOG.nonReactiveMethodCall( "reactiveAssemble" );
	}

	@Override
	default J assemble(RowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		throw LOG.nonReactiveMethodCall( "reactiveAssemble" );
	}

	CompletionStage<J> reactiveAssemble(
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options);

	/**
	 * Convenience form of {@link #assemble(RowProcessingState, JdbcValuesSourceProcessingOptions)}
	 */
	default CompletionStage<J> reactiveAssemble(ReactiveRowProcessingState rowProcessingState) {
		return reactiveAssemble(
				rowProcessingState,
				rowProcessingState.getJdbcValuesSourceProcessingState().getProcessingOptions()
		);
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.engine.internal.ReactivePersistenceContextAdapter;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;

@Incubating
public class ReactiveSingleResultConsumer<T> implements ReactiveResultsConsumer<T, T> {

	@Override
	public CompletionStage<T> consume(
			ReactiveValuesResultSet jdbcValues,
			SharedSessionContractImplementor session,
			JdbcValuesSourceProcessingOptions processingOptions,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			ReactiveRowProcessingState rowProcessingState,
			ReactiveRowReader<T> rowReader) {
		rowReader.startLoading( rowProcessingState );
		return rowProcessingState.next()
				.thenCompose( hasNext -> rowReader
						.reactiveReadRow( rowProcessingState, processingOptions )
						.thenCompose( result -> {
							rowProcessingState.finishRowProcessing( true );
							rowReader.finishUp( rowProcessingState );
							session.getPersistenceContext();
							jdbcValuesSourceProcessingState.finishLoadingCollections();
							return ( (ReactivePersistenceContextAdapter) session.getPersistenceContextInternal() )
									.reactivePostLoad( jdbcValuesSourceProcessingState, null )
									.thenApply( v -> result );
						} )
				);
	}

	@Override
	public boolean canResultsBeCached() {
		return false;
	}

}

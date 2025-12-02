/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.engine.internal.ReactivePersistenceContextAdapter;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;

/**
 * @see org.hibernate.sql.results.spi.SingleResultConsumer
 */
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
							return finishUp( session, jdbcValuesSourceProcessingState, result );
						} )
				);
	}

	/**
	 * Reactive version of {@link JdbcValuesSourceProcessingStateStandardImpl#finishUp(boolean)}
	 */
	private static <T> CompletionStage<T> finishUp(
			SharedSessionContractImplementor session,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			T result) {
		jdbcValuesSourceProcessingState.finishLoadingCollections();
		final ExecutionContext executionContext = jdbcValuesSourceProcessingState.getExecutionContext();
		return ( (ReactivePersistenceContextAdapter) session.getPersistenceContextInternal() )
				.reactivePostLoad( jdbcValuesSourceProcessingState, executionContext::registerLoadingEntityHolder )
				.thenApply( v -> result );
	}

	@Override
	public boolean canResultsBeCached() {
		return false;
	}

}

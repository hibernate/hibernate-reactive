/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveResultsConsumer;
import org.hibernate.sql.exec.internal.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.internal.StandardStatementCreator;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.exec.spi.JdbcSelectExecutor;
import org.hibernate.sql.results.spi.RowTransformer;

/**
 * @see org.hibernate.sql.exec.spi.JdbcSelectExecutor
 */
public interface ReactiveSelectExecutor {

	/**
	 * @since 2.4 (and Hibernate ORM 6.6)
	 */
	default <T, R> CompletionStage<T> executeQuery(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			int resultCountEstimate,
			ReactiveResultsConsumer<T, R> resultsConsumer) {
		return executeQuery(
				jdbcSelect,
				jdbcParameterBindings,
				executionContext,
				rowTransformer,
				domainResultType,
				resultCountEstimate,
				StandardStatementCreator.getStatementCreator( null ),
				resultsConsumer
		);
	}

	<T, R> CompletionStage<T> executeQuery(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			int resultCountEstimate,
			JdbcSelectExecutor.StatementCreator statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer);

	<R> CompletionStage<List<R>> list(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic);

}

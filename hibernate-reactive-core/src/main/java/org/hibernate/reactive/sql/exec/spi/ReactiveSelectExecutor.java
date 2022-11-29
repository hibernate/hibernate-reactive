/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveResultsConsumer;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.spi.RowTransformer;

/**
 * @see org.hibernate.sql.exec.spi.JdbcSelectExecutor
 */
public interface ReactiveSelectExecutor {

	<T, R> CompletionStage<T> executeQuery(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			Function<String, PreparedStatement> statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer);

	<R> CompletionStage<List<R>> list(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic);

}

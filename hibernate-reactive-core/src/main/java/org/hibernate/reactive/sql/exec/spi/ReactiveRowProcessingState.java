/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.results.internal.ReactiveInitializersList;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.EntityFetch;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingState;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.sql.results.spi.RowReader;


/**
 * @see org.hibernate.sql.results.internal.RowProcessingStateStandardImpl
 */
public class ReactiveRowProcessingState extends BaseExecutionContext implements RowProcessingState {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final JdbcValuesSourceProcessingStateStandardImpl resultSetProcessingState;

	private final ReactiveInitializersList initializers;

	private final ReactiveRowReader<?> rowReader;
	private final ReactiveValuesResultSet jdbcValues;
	private final ExecutionContext executionContext;

	public ReactiveRowProcessingState(
			JdbcValuesSourceProcessingStateStandardImpl resultSetProcessingState,
			ExecutionContext executionContext,
			ReactiveRowReader<?> rowReader,
			ReactiveValuesResultSet jdbcValues) {
		super( resultSetProcessingState.getSession() );
		this.resultSetProcessingState = resultSetProcessingState;
		this.executionContext = executionContext;
		this.rowReader = rowReader;
		this.initializers = rowReader.getReactiveInitializersList();
		this.jdbcValues = jdbcValues;
	}

	public CompletionStage<Boolean> next() {
		return jdbcValues.next();
	}

	@Override
	public JdbcValuesSourceProcessingState getJdbcValuesSourceProcessingState() {
		return resultSetProcessingState;
	}

	@Override
	public RowReader<?> getRowReader() {
		return rowReader;
	}

	@Override
	public Object getJdbcValue(int position) {
		return jdbcValues.getCurrentRowValuesArray()[position];
	}

	@Override
	public void registerNonExists(EntityFetch fetch) {
	}

	@Override
	public boolean isQueryCacheHit() {
		// TODO [ORM-6]: Implements cached version
//		return jdbcValues instanceof JdbcValuesCacheHit;
		return false;
	}

	public void finishRowProcessing() {
	}

	@Override
	public Initializer resolveInitializer(NavigablePath path) {
		return this.initializers.resolveInitializer( path );
	}

	public QueryOptions getQueryOptions() {
		return this.executionContext.getQueryOptions();
	}

	public boolean hasCollectionInitializers() {
		return this.initializers.hasCollectionInitializers();
	}
}

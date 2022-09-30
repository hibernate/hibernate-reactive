/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.collection.CollectionInitializer;
import org.hibernate.sql.results.graph.entity.EntityFetch;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingState;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.sql.results.spi.RowReader;


/**
 * @see org.hibernate.sql.results.internal.RowProcessingStateStandardImpl
 */
public class ReactiveRowProcessingState implements RowProcessingState {

	private static final Initializer[] NO_INITIALIZERS = new Initializer[0];

	private final JdbcValuesSourceProcessingStateStandardImpl resultSetProcessingState;

	private final Initializer[] initializers;

	private final RowReader<?> rowReader;
	private final ReactiveValuesResultSet jdbcValues;
	private final ExecutionContext executionContext;
	public final boolean hasCollectionInitializers;

	public ReactiveRowProcessingState(
			JdbcValuesSourceProcessingStateStandardImpl resultSetProcessingState,
			ExecutionContext executionContext,
			RowReader<?> rowReader,
			ReactiveValuesResultSet jdbcValues) {
		this.resultSetProcessingState = resultSetProcessingState;
		this.executionContext = executionContext;
		this.rowReader = rowReader;
		this.jdbcValues = jdbcValues;

		final List<Initializer> initializers = rowReader.getInitializers();
		if ( initializers == null || initializers.isEmpty() ) {
			this.initializers = NO_INITIALIZERS;
			hasCollectionInitializers = false;
		}
		else {
			//noinspection ToArrayCallWithZeroLengthArrayArgument
			this.initializers = initializers.toArray( new Initializer[initializers.size()] );
			hasCollectionInitializers = hasCollectionInitializers(this.initializers);
		}
	}

	private static boolean hasCollectionInitializers(Initializer[] initializers) {
		for ( int i = 0; i < initializers.length; i++ ) {
			if ( initializers[i] instanceof CollectionInitializer ) {
				return true;
			}
		}
		return false;
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
//		return jdbcValues instanceof JdbcValuesCacheHit;
		return false; // Not implemeted yet for reactive
	}

	public void finishRowProcessing() {
	}

	@Override
	public Initializer resolveInitializer(NavigablePath path) {
		return null;
	}

	@Override
	public SharedSessionContractImplementor getSession() {
		return null;
	}

	public QueryOptions getQueryOptions() {
		return this.executionContext.getQueryOptions();
	}

	@Override
	public QueryParameterBindings getQueryParameterBindings() {
		return null;
	}

	@Override
	public Callback getCallback() {
		return null;
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.entity.EntityFetch;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingState;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.sql.results.spi.RowReader;


/**
 * @see org.hibernate.sql.results.internal.RowProcessingStateStandardImpl
 */
public class ReactiveRowProcessingState extends BaseExecutionContext implements RowProcessingState {

	private final JdbcValuesSourceProcessingStateStandardImpl resultSetProcessingState;

	private final ReactiveRowReader<?> rowReader;
	private final ReactiveValuesResultSet jdbcValues;
	private final ExecutionContext executionContext;
	private final boolean needsResolveState;

	private final InitializerData[] initializerData;

	public ReactiveRowProcessingState(
			JdbcValuesSourceProcessingStateStandardImpl resultSetProcessingState,
			ExecutionContext executionContext,
			ReactiveRowReader<?> rowReader,
			ReactiveValuesResultSet jdbcValues) {
		super( resultSetProcessingState.getSession() );
		this.resultSetProcessingState = resultSetProcessingState;
		this.executionContext = executionContext;
		this.rowReader = rowReader;
		this.jdbcValues = jdbcValues;
		this.needsResolveState = !isQueryCacheHit()
				&& getQueryOptions().isResultCachingEnabled() == Boolean.TRUE;
		this.initializerData = new InitializerData[rowReader.getInitializerCount()];
	}

	public CompletionStage<Boolean> next() {
		return jdbcValues.next();
	}

	@Override
	public JdbcValuesSourceProcessingState getJdbcValuesSourceProcessingState() {
		return resultSetProcessingState;
	}

	@Override
	public Object getEntityId() {
		return executionContext.getEntityId();
	}

	@Override
	public LockMode determineEffectiveLockMode(String alias) {
		if ( jdbcValues.usesFollowOnLocking() ) {
			// If follow-on locking is used, we must omit the lock options here,
			// because these lock options are only for Initializers.
			// If we wouldn't omit this, the follow-on lock requests would be no-ops,
			// because the EntityEntrys would already have the desired lock mode
			return LockMode.NONE;
		}
		final LockMode effectiveLockMode = resultSetProcessingState.getQueryOptions().getLockOptions()
				.getEffectiveLockMode( alias );
		return effectiveLockMode == LockMode.NONE
				? jdbcValues.getValuesMapping().determineDefaultLockMode( alias, effectiveLockMode )
				: effectiveLockMode;
	}

	@Override
	public boolean needsResolveState() {
		return needsResolveState;
	}

	@Override
	public <T extends InitializerData> T getInitializerData(int initializerId) {
		return (T) initializerData[initializerId];
	}

	@Override
	public void setInitializerData(int initializerId, InitializerData state) {
		initializerData[initializerId] = state;
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

	@Override
	public void finishRowProcessing(boolean wasAdded) {
		jdbcValues.finishRowProcessing( this, wasAdded );
	}

	public QueryOptions getQueryOptions() {
		return this.executionContext.getQueryOptions();
	}
}

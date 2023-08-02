/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.HibernateException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.exec.ExecutionException;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.caching.QueryCachePutManager;
import org.hibernate.sql.results.caching.internal.QueryCachePutManagerDisabledImpl;
import org.hibernate.sql.results.caching.internal.QueryCachePutManagerEnabledImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;

/**
 * @see org.hibernate.sql.results.jdbc.internal.JdbcValuesResultSetImpl
 */
public class ReactiveValuesResultSet {

	private final QueryCachePutManager queryCachePutManager;

	private final ReactiveResultSetAccess resultSetAccess;
	private final JdbcValuesMapping valuesMapping;
	private final ExecutionContext executionContext;
	private final SqlSelection[] sqlSelections;
	private final Object[] currentRowJdbcValues;

	public ReactiveValuesResultSet(
			ReactiveResultSetAccess resultSetAccess,
			QueryKey queryCacheKey,
			String queryIdentifier,
			QueryOptions queryOptions,
			JdbcValuesMapping valuesMapping,
			JdbcValuesMetadata metadataForCache,
			ExecutionContext executionContext) {
		this.queryCachePutManager = resolveQueryCachePutManager( executionContext, queryOptions, queryCacheKey, queryIdentifier, metadataForCache );
		this.resultSetAccess = resultSetAccess;
		this.valuesMapping = valuesMapping;
		this.executionContext = executionContext;
		this.sqlSelections = valuesMapping.getSqlSelections().toArray( new SqlSelection[0] );
		this.currentRowJdbcValues = new Object[ valuesMapping.getRowSize() ];
	}

	private static QueryCachePutManager resolveQueryCachePutManager(
			ExecutionContext executionContext,
			QueryOptions queryOptions,
			QueryKey queryCacheKey,
			String queryIdentifier,
			JdbcValuesMetadata metadataForCache) {
		if ( queryCacheKey == null ) {
			return QueryCachePutManagerDisabledImpl.INSTANCE;
		}

		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final QueryResultsCache queryCache = factory.getCache()
				.getQueryResultsCache( queryOptions.getResultCacheRegionName() );
		return new QueryCachePutManagerEnabledImpl( queryCache, factory.getStatistics(), queryCacheKey, queryIdentifier, metadataForCache );
	}

	public final InternalStage<Boolean> next() {
		return processNext()
				.thenApply( hadRow -> {
					if ( hadRow ) {
						queryCachePutManager.registerJdbcRow( getCurrentRowValuesArray() );
					}
					return hadRow;
				} );
	}

	protected final InternalStage<Boolean> processNext() {
		return advance( () -> resultSetAccess
				.getReactiveResultSet()
				.thenCompose( this::doNext )
		);
	}

	private InternalStage<Boolean> doNext(ResultSet resultSet) {
		try {
			return completedFuture( resultSet.next() );
		}
		catch (SQLException e) {
			return failedFuture( makeExecutionException( "Error advancing (next) ResultSet position", e ) );
		}
	}

	private ExecutionException makeExecutionException(String message, SQLException cause) {
		return new ExecutionException(
				message,
				executionContext.getSession().getJdbcServices().getSqlExceptionHelper().convert( cause, message )
		);
	}

	public JdbcValuesMapping getValuesMapping() {
		return valuesMapping;
	}

	public Object[] getCurrentRowValuesArray() {
		return currentRowJdbcValues;
	}

	public void finishUp(SharedSessionContractImplementor session) {
		queryCachePutManager.finishUp( session );
	}

	@FunctionalInterface
	private interface Advancer {
		InternalStage<Boolean> advance();
	}

	private InternalStage<Boolean> advance(Advancer advancer) {
		return advancer
				.advance()
				.thenCompose( this::readCurrentRowValues );
	}

	private InternalStage<Boolean> readCurrentRowValues(boolean hasResults) {
		if ( !hasResults ) {
			return falseFuture();
		}

		return resultSetAccess.getReactiveResultSet()
				.thenApply( resultSet -> {
					final SharedSessionContractImplementor session = executionContext.getSession();
					for ( final SqlSelection sqlSelection : sqlSelections ) {
						try {
							currentRowJdbcValues[ sqlSelection.getValuesArrayPosition() ] = sqlSelection
									.getJdbcValueExtractor()
									.extract( resultSet, sqlSelection.getJdbcResultSetIndex(), session );
						}
						catch (Exception e) {
							throw new HibernateException( "Unable to extract JDBC value for position `" + sqlSelection.getJdbcResultSetIndex() + "`", e );
						}
					}
					return true;
				} );
	}
}

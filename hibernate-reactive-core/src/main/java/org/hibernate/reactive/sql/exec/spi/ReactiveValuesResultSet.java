/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.QueryTimeoutException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.exception.DataException;
import org.hibernate.exception.LockTimeoutException;
import org.hibernate.query.spi.Limit;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.sql.ast.spi.query.select.SqlSelection;
import org.hibernate.sql.exec.ExecutionException;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.caching.internal.QueryCachePutManager;
import org.hibernate.sql.results.caching.internal.QueryCachePutManagerEnabledImpl;
import org.hibernate.sql.results.jdbc.internal.CachedJdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesCacheHit;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.reactive.util.internal.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.internal.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.internal.CompletionStages.falseFuture;

/**
 * @see org.hibernate.sql.results.jdbc.internal.JdbcValuesResultSetImpl
 */
public class ReactiveValuesResultSet {

	private final QueryCachePutManager queryCachePutManager;
	// Only set when the values are read from the query cache
	private final JdbcValuesCacheHit cacheHit;

	private final ReactiveResultSetAccess resultSetAccess;
	private final JdbcValuesMapping valuesMapping;
	private final ExecutionContext executionContext;
	private final boolean usesFollowOnLocking;
	private final int resultCountEstimate;
	private final SqlSelection[] sqlSelections;
	private final BitSet initializedIndexes;
	private final Object[] currentRowJdbcValues;
	private final int[] valueIndexesToCacheIndexes;
	// Is only meaningful if valueIndexesToCacheIndexes is not null
	// Contains the size of the row to cache, or if the value is negative,
	// represents the inverted index of the single value to cache
	private final int rowToCacheSize;
	private int resultCount;

	public ReactiveValuesResultSet(
			ReactiveResultSetAccess resultSetAccess,
			QueryKey queryCacheKey,
			String queryIdentifier,
			QueryOptions queryOptions,
			boolean usesFollowOnLocking,
			JdbcValuesMapping valuesMapping,
			CachedJdbcValuesMetadata metadataForCache,
			ExecutionContext executionContext) {
		this.queryCachePutManager = resolveQueryCachePutManager(
				executionContext,
				queryOptions,
				queryCacheKey,
				queryIdentifier,
				metadataForCache
		);
		this.cacheHit = null;
		this.resultSetAccess = resultSetAccess;
		this.valuesMapping = valuesMapping;
		this.executionContext = executionContext;
		this.usesFollowOnLocking = usesFollowOnLocking;
		this.resultCountEstimate = determineResultCountEstimate( resultSetAccess, queryOptions );

		final int rowSize = valuesMapping.getRowSize();
		this.sqlSelections = new SqlSelection[rowSize];
		for ( SqlSelection selection : valuesMapping.getSqlSelections() ) {
			int valuesArrayPosition = selection.getValuesArrayPosition();
			this.sqlSelections[valuesArrayPosition] = selection;
		}
		this.initializedIndexes = new BitSet( rowSize );
		this.currentRowJdbcValues = new Object[rowSize];
		if ( queryCachePutManager == null ) {
			this.valueIndexesToCacheIndexes = null;
			this.rowToCacheSize = -1;
		}
		else {
			this.valueIndexesToCacheIndexes = valuesMapping.getValueIndexesToCacheIndexes();
			final int rowToCacheSize = valuesMapping.getRowToCacheSize();
			assert rowToCacheSize > 0;
			int cacheIndex = rowToCacheSize;
			if ( rowToCacheSize == 1 ) {
				// Special case. Set the rowToCacheSize to the inverted index of the single element to cache
				for ( int i = 0; i < valueIndexesToCacheIndexes.length; i++ ) {
					if ( valueIndexesToCacheIndexes[i] != -1 ) {
						cacheIndex = -i;
						break;
					}
				}
			}
			this.rowToCacheSize = cacheIndex;
		}
	}

	/**
	 * Creates a result set that reads the rows from the query cache instead of the database.
	 *
	 * @see org.hibernate.sql.results.jdbc.internal.JdbcValuesCacheHit
	 */
	public ReactiveValuesResultSet(JdbcValuesCacheHit cacheHit, ExecutionContext executionContext) {
		this.cacheHit = cacheHit;
		this.queryCachePutManager = null;
		this.resultSetAccess = null;
		this.valuesMapping = cacheHit.getValuesMapping();
		this.executionContext = executionContext;
		this.usesFollowOnLocking = cacheHit.usesFollowOnLocking();
		this.resultCountEstimate = cacheHit.getResultCountEstimate();
		this.sqlSelections = new SqlSelection[0];
		this.initializedIndexes = new BitSet();
		this.currentRowJdbcValues = new Object[0];
		this.valueIndexesToCacheIndexes = null;
		this.rowToCacheSize = -1;
	}

	private int determineResultCountEstimate(
			ReactiveResultSetAccess resultSetAccess,
			QueryOptions queryOptions) {
		final Limit limit = queryOptions.getLimit();
		if ( limit != null && limit.getMaxRows() != null ) {
			return limit.getMaxRows();
		}

		final int resultCountEstimate = resultSetAccess.getResultCountEstimate();
		if ( resultCountEstimate > 0 ) {
			return resultCountEstimate;
		}
		return -1;
	}

	private static QueryCachePutManager resolveQueryCachePutManager(
			ExecutionContext executionContext,
			QueryOptions queryOptions,
			QueryKey queryCacheKey,
			String queryIdentifier,
			CachedJdbcValuesMetadata metadataForCache) {
		if ( queryCacheKey != null ) {
			final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
			final QueryResultsCache queryCache = factory.getCache()
					.getQueryResultsCache( queryOptions.getResultCacheRegionName() );
			return new QueryCachePutManagerEnabledImpl(
					queryCache,
					factory.getStatistics(),
					queryCacheKey,
					queryIdentifier,
					metadataForCache
			);
		}
		return null;
	}

	public final CompletionStage<Boolean> next() {
		return processNext();
	}

	protected final CompletionStage<Boolean> processNext() {
		if ( cacheHit != null ) {
			return completedFuture( cacheHit.next( null ) );
		}
		return advance( () -> resultSetAccess
				.getReactiveResultSet()
				.thenCompose( this::doNext )
		);
	}

	private CompletionStage<Boolean> doNext(ResultSet resultSet) {
		try {
			boolean next = resultSet.next();
			return completedFuture( next );
		}
		catch (SQLException e) {
			return failedFuture( makeExecutionException( "Error advancing (next) ResultSet position", e ) );
		}
	}

	// Copied from JdbcValuesResultSetImpl#makeExecutionException, not sure if we can actually have a JDBCException
	private ExecutionException makeExecutionException(String message, SQLException cause) {
		final JDBCException jdbcException = executionContext.getSession().getJdbcServices()
				.getSqlExceptionHelper().convert( cause, message );
		if ( jdbcException instanceof QueryTimeoutException
				|| jdbcException instanceof DataException
				|| jdbcException instanceof LockTimeoutException ) {
			// So far, the exception helper threw these exceptions more or less directly during conversion,
			// so to retain the same behavior, we throw that directly now as well instead of wrapping it
			throw jdbcException;
		}
		return new ExecutionException( message + " [" + cause.getMessage() + "]", jdbcException );
	}

	public JdbcValuesMapping getValuesMapping() {
		return valuesMapping;
	}

	public Object getCurrentRowValue(int valueIndex) {
		return cacheHit != null
				? cacheHit.getCurrentRowValue( valueIndex )
				: currentRowJdbcValues[valueIndex];
	}

	public boolean isQueryCacheHit() {
		return cacheHit != null;
	}

	public void finishUp(SharedSessionContractImplementor session) {
		if ( cacheHit != null ) {
			cacheHit.finishUp( session );
			return;
		}
		if ( queryCachePutManager != null ) {
			queryCachePutManager.finishUp( resultCount, session );
		}
		resultSetAccess.release();
	}

	public boolean usesFollowOnLocking() {
		return usesFollowOnLocking;
	}

	public void finishRowProcessing(RowProcessingState rowProcessingState, boolean wasAdded) {
		if ( queryCachePutManager != null ) {
			if ( wasAdded ) {
				resultCount++;
			}
			final Object objectToCache;
			if ( valueIndexesToCacheIndexes == null ) {
				objectToCache = Arrays.copyOf( currentRowJdbcValues, currentRowJdbcValues.length );
			}
			else if ( rowToCacheSize < 1 ) {
				if ( !wasAdded ) {
					// skip adding duplicate objects
					return;
				}
				objectToCache = currentRowJdbcValues[-rowToCacheSize];
			}
			else {
				final Object[] rowToCache = new Object[rowToCacheSize];
				for ( int i = 0; i < valueIndexesToCacheIndexes.length; i++ ) {
					final int cacheIndex = valueIndexesToCacheIndexes[i];
					if ( cacheIndex != -1 ) {
						rowToCache[cacheIndex] = initializedIndexes.get( i ) ? currentRowJdbcValues[i] : null;
					}
				}
				objectToCache = rowToCache;
			}
			queryCachePutManager.registerJdbcRow( objectToCache );
		}
	}

	@FunctionalInterface
	private interface Advancer {
		CompletionStage<Boolean> advance();
	}

	private CompletionStage<Boolean> advance(Advancer advancer) {
		return advancer
				.advance()
				.thenCompose( this::readCurrentRowValues );
	}

	private CompletionStage<Boolean> readCurrentRowValues(boolean hasResults) {
		if ( !hasResults ) {
			return falseFuture();
		}

		return resultSetAccess.getReactiveResultSet()
				.thenApply( resultSet -> {
					final SharedSessionContractImplementor session = executionContext.getSession();
					initializedIndexes.clear();
					for ( final SqlSelection sqlSelection : sqlSelections ) {
						// The array can be sparse (it's sized by the row size, not by the number of selections),
						// ORM never reads the missing positions, so we skip them
						if ( sqlSelection != null ) {
							final int valueIndex = sqlSelection.getValuesArrayPosition();
							try {
								currentRowJdbcValues[valueIndex] = sqlSelection
										.getJdbcValueExtractor()
										.extract( resultSet, sqlSelection.getJdbcResultSetIndex(), session );
								initializedIndexes.set( valueIndex );
							}
							catch (Exception e) {
								throw new HibernateException( "Unable to extract JDBC value for position `" + sqlSelection.getJdbcResultSetIndex() + "`", e );
							}
						}
					}
					return true;
				} );
	}

	public int getResultCountEstimate() {
		return resultCountEstimate;
	}
}

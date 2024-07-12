/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
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
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.exec.ExecutionException;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.caching.QueryCachePutManager;
import org.hibernate.sql.results.caching.internal.QueryCachePutManagerEnabledImpl;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

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
	private final boolean usesFollowOnLocking;

	private final SqlSelection[] sqlSelections;
	private final BitSet initializedIndexes;
	private final Object[] currentRowJdbcValues;
	private final int[] valueIndexesToCacheIndexes;
	// Is only meaningful if valueIndexesToCacheIndexes is not null
	// Contains the size of the row to cache, or if the value is negative,
	// represents the inverted index of the single value to cache
	private final int rowToCacheSize;

	public ReactiveValuesResultSet(
			ReactiveResultSetAccess resultSetAccess,
			QueryKey queryCacheKey,
			String queryIdentifier,
			QueryOptions queryOptions,
			boolean usesFollowOnLocking,
			JdbcValuesMapping valuesMapping,
			JdbcValuesMetadata metadataForCache,
			ExecutionContext executionContext) {
		this.queryCachePutManager = resolveQueryCachePutManager( executionContext, queryOptions, queryCacheKey, queryIdentifier, metadataForCache );
		this.resultSetAccess = resultSetAccess;
		this.valuesMapping = valuesMapping;
		this.executionContext = executionContext;
		this.usesFollowOnLocking = usesFollowOnLocking;

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
			final BitSet valueIndexesToCache = new BitSet( rowSize );
			for ( DomainResult<?> domainResult : valuesMapping.getDomainResults() ) {
				domainResult.collectValueIndexesToCache( valueIndexesToCache );
			}
			if ( valueIndexesToCache.nextClearBit( 0 ) == -1 ) {
				this.valueIndexesToCacheIndexes = null;
				this.rowToCacheSize = -1;
			}
			else {
				final int[] valueIndexesToCacheIndexes = new int[rowSize];
				int cacheIndex = 0;
				for ( int i = 0; i < valueIndexesToCacheIndexes.length; i++ ) {
					if ( valueIndexesToCache.get( i ) ) {
						valueIndexesToCacheIndexes[i] = cacheIndex++;
					}
					else {
						valueIndexesToCacheIndexes[i] = -1;
					}
				}

				this.valueIndexesToCacheIndexes = valueIndexesToCacheIndexes;
				if ( cacheIndex == 1 ) {
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
	}

	private static QueryCachePutManager resolveQueryCachePutManager(
			ExecutionContext executionContext,
			QueryOptions queryOptions,
			QueryKey queryCacheKey,
			String queryIdentifier,
			JdbcValuesMetadata metadataForCache) {
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

	public Object[] getCurrentRowValuesArray() {
		return currentRowJdbcValues;
	}

	public void finishUp(SharedSessionContractImplementor session) {
		if ( queryCachePutManager != null ) {
			queryCachePutManager.finishUp( session );
		}
		resultSetAccess.release();
	}

	public boolean usesFollowOnLocking() {
		return usesFollowOnLocking;
	}

	public void finishRowProcessing(RowProcessingState rowProcessingState, boolean wasAdded) {
		if ( queryCachePutManager != null ) {
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
				for ( int i = 0; i < currentRowJdbcValues.length; i++ ) {
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
					for ( final SqlSelection sqlSelection : sqlSelections ) {
						try {
							currentRowJdbcValues[sqlSelection.getValuesArrayPosition()] = sqlSelection
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

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.hibernate.CacheMode;
import org.hibernate.SharedSessionContract;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.TupleTransformer;
import org.hibernate.reactive.engine.internal.ReactivePersistenceContextAdapter;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.sql.exec.spi.ReactiveJdbcSelect;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveSelectExecutor;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.reactive.sql.results.internal.ReactiveDeferredResultSetAccess;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.reactive.sql.results.internal.ReactiveResultsHelper;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.reactive.sql.results.spi.ReactiveValuesMappingProducer;
import org.hibernate.sql.exec.internal.StandardStatementCreator;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.exec.spi.JdbcSelectExecutor;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;
import org.hibernate.sql.results.internal.RowTransformerTupleTransformerAdapter;
import org.hibernate.sql.results.jdbc.internal.CachedJdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesCacheHit;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.RowTransformer;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.internal.util.NullnessHelper.coalesceSuppliedValues;
import static org.hibernate.internal.util.collections.ArrayHelper.indexOf;
import static org.hibernate.reactive.util.internal.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.internal.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.internal.CompletionStages.rethrow;
import static org.hibernate.sql.exec.SqlExecLogger.SQL_EXEC_LOGGER;

/**
 * @see org.hibernate.sql.exec.internal.JdbcSelectExecutorStandardImpl
 */
public class StandardReactiveSelectExecutor implements ReactiveSelectExecutor {

	public static final StandardReactiveSelectExecutor INSTANCE = new StandardReactiveSelectExecutor();

	private StandardReactiveSelectExecutor() {
	}

	public <R> CompletionStage<List<R>> list(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic) {
		return list( jdbcSelect, jdbcParameterBindings, executionContext, rowTransformer, null, uniqueSemantic );
	}

	public <R> CompletionStage<List<R>> list(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic) {
		return list(
				jdbcSelect,
				jdbcParameterBindings,
				executionContext,
				rowTransformer,
				domainResultType,
				uniqueSemantic,
				-1
		);
	}

	/**
	 * @since 2.4 (and ORM 6.6)
	 */
	public  <R> CompletionStage<List<R>> list(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> requestedJavaType,
			ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic,
			int resultCountEstimate) {
		// Only do auto flushing for top level queries
		return executeQuery(
				jdbcSelect,
				jdbcParameterBindings,
				executionContext,
				rowTransformer,
				requestedJavaType,
				resultCountEstimate,
				ReactiveListResultsConsumer.instance( uniqueSemantic )
		);
	}

	/**
	 * @since 2.4 (and Hibernate ORM 6.6)
	 */
	public  <T, R> CompletionStage<T> executeQuery(
			JdbcSelect jdbcSelect,
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

	@Override
	public <T, R> CompletionStage<T> executeQuery(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			int resultCountEstimate,
			JdbcSelectExecutor.StatementCreator statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer) {

		final PersistenceContext persistenceContext = executionContext.getSession().getPersistenceContext();
		final boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
		final Boolean readOnly = executionContext.getQueryOptions().isReadOnly();
		if ( readOnly != null ) {
			// The read-only/modifiable mode for the query was explicitly set.
			// Temporarily set the default read-only/modifiable setting to the query's setting.
			persistenceContext.setDefaultReadOnly( readOnly );
		}

 		return doExecuteQuery( jdbcSelect, jdbcParameterBindings, executionContext, rowTransformer, domainResultType, resultCountEstimate, statementCreator, resultsConsumer )
				.thenCompose( list -> ( (ReactivePersistenceContextAdapter) persistenceContext )
						// only initialize non-lazy collections after everything else has been refreshed
						.reactiveInitializeNonLazyCollections()
						.thenApply( v -> list )
				)
				.whenComplete( (o, throwable) -> {
					if ( readOnly != null ) {
						persistenceContext.setDefaultReadOnly( defaultReadOnlyOrig );
					}
				} );
	}

	private <T, R> CompletionStage<T> doExecuteQuery(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> transformer,
			Class<R> domainResultType,
			int resultCountEstimate,
			JdbcSelectExecutor.StatementCreator statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer) {

		final ReactiveDeferredResultSetAccess deferredResultSetAccess =
				new ReactiveDeferredResultSetAccess( jdbcSelect, jdbcParameterBindings, executionContext, statementCreator, resultCountEstimate );

		return resolveJdbcValuesSource(
				executionContext.getQueryIdentifier( deferredResultSetAccess.getFinalSql() ),
				jdbcSelect,
				resultsConsumer.canResultsBeCached(),
				executionContext,
				deferredResultSetAccess )
				.thenCompose( jdbcValues -> {
					final RowTransformer<R> rowTransformer = rowTransformer( executionContext, transformer, jdbcValues );
					final Statistics statistics = new Statistics( executionContext, jdbcValues );

					/*
					 * Processing options effectively are only used for entity loading.  Here we don't need these values.
					 */
					final JdbcValuesSourceProcessingOptions processingOptions = new JdbcValuesSourceProcessingOptions() {
						@Override
						public Object getEffectiveOptionalObject() {
							return executionContext.getEntityInstance();
						}

						@Override
						public String getEffectiveOptionalEntityName() {
							return null;
						}

						@Override
						public Object getEffectiveOptionalId() {
							return executionContext.getEntityId();
						}

						@Override
						public boolean shouldReturnProxies() {
							return true;
						}
					};

					final var loadedValuesCollector = jdbcSelect.getLoadedValuesCollectorFactory() == null
							? null
							: jdbcSelect.getLoadedValuesCollectorFactory().build();
					final JdbcValuesSourceProcessingStateStandardImpl valuesProcessingState =
							new JdbcValuesSourceProcessingStateStandardImpl(
									loadedValuesCollector,
									processingOptions,
									executionContext
							);

					final SharedSessionContractImplementor session = executionContext.getSession();
					final ReactiveRowReader<R> rowReader = ReactiveResultsHelper.createRowReader(
							session.getSessionFactory(),
							rowTransformer,
							domainResultType,
							jdbcValues
					);

					final ReactiveRowProcessingState rowProcessingState = new ReactiveRowProcessingState(
							valuesProcessingState,
							executionContext,
							rowReader,
							jdbcValues
					);

					rowReader.startLoading( rowProcessingState );
					ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
					if ( jdbcSelect instanceof ReactiveJdbcSelect reactiveJdbcSelect ) {
						return reactiveJdbcSelect.reactivePerformPreActions( reactiveConnection, executionContext )
								.thenCompose( unused -> resultsConsumer
										.consume(
												jdbcValues,
												session,
												processingOptions,
												valuesProcessingState,
												rowProcessingState,
												rowReader
										) )
								.thenCompose( result -> reactiveJdbcSelect
										.reactivePerformPostActions( true, reactiveConnection, executionContext, loadedValuesCollector )
										.thenApply( v -> {
											statistics.end( jdbcSelect, result );
											return result;
										} )
								);
					}
					else {
						return resultsConsumer.consume( jdbcValues, session, processingOptions, valuesProcessingState, rowProcessingState, rowReader )
								.thenApply( result -> {
									statistics.end( jdbcSelect, result );
									return result;
								} );
					}
		});
	}

	private static <R> RowTransformer<R> rowTransformer(
			ExecutionContext executionContext,
			RowTransformer<R> transformer,
			ReactiveValuesResultSet jdbcValues) {
		if ( transformer == null ) {
			@SuppressWarnings("unchecked")
			final TupleTransformer<R> tupleTransformer =
					(TupleTransformer<R>) executionContext.getQueryOptions().getTupleTransformer();
			if ( tupleTransformer == null ) {
				return RowTransformerStandardImpl.instance();
			}
			else {
				final List<DomainResult<?>> domainResults = jdbcValues.getValuesMapping().getDomainResults();
				final String[] aliases = new String[domainResults.size()];
				for ( int i = 0; i < domainResults.size(); i++ ) {
					aliases[i] = domainResults.get( i ).getResultVariable();
				}
				return new RowTransformerTupleTransformerAdapter<>( aliases, tupleTransformer );
			}
		}
		else {
			return transformer;
		}
	}

	public CompletionStage<ReactiveValuesResultSet> resolveJdbcValuesSource(
			String queryIdentifier,
			JdbcSelect jdbcSelect,
			boolean canBeCached,
			ExecutionContext executionContext,
			ReactiveDeferredResultSetAccess resultSetAccess) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		final boolean queryCacheEnabled = factory.getSessionFactoryOptions().isQueryCacheEnabled();

		final CacheMode cacheMode = resolveCacheMode( executionContext );
		final QueryOptions queryOptions = executionContext.getQueryOptions();
		final boolean cacheable = queryCacheEnabled
				&& canBeCached
				&& queryOptions.isResultCachingEnabled() == Boolean.TRUE;

		final QueryKey queryResultsCacheKey;
		final List<?> cachedResults;
		final String queryCacheRegionName;
		if ( cacheable && cacheMode.isGetEnabled() ) {
			SQL_EXEC_LOGGER.readingQueryResultCacheData( cacheMode.name() );
			final Set<String> querySpaces = jdbcSelect.getAffectedTableNames();
			if ( querySpaces == null || querySpaces.isEmpty() ) {
				SQL_EXEC_LOGGER.affectedQuerySpacesUnexpectedlyEmpty();
			}
			else {
				SQL_EXEC_LOGGER.affectedQuerySpaces( querySpaces );
			}

			final QueryResultsCache queryCache = factory.getCache()
					.getQueryResultsCache( queryOptions.getResultCacheRegionName() );
			queryCacheRegionName = queryCache.getRegion().getName();

			queryResultsCacheKey = QueryKey
					.from( jdbcSelect.getSqlString(), queryOptions.getLimit(), executionContext.getQueryParameterBindings(), session );

			cachedResults = queryCache.get(
					// todo (6.0) : QueryCache#get takes the `queryResultsCacheKey` see tat discussion above
					queryResultsCacheKey,
					// todo (6.0) : `querySpaces` and `session` make perfect sense as args, but its odd passing those into this method just to pass along
					//		atm we do not even collect querySpaces, but we need to
					querySpaces,
					session
			);

			// todo (6.0) : `querySpaces` and `session` are used in QueryCache#get to verify "up-to-dateness" via UpdateTimestampsCache
			//		better imo to move UpdateTimestampsCache handling here and have QueryCache be a simple access to
			//		the underlying query result cache region.
			//
			// todo (6.0) : if we go this route (^^), still beneficial to have an abstraction over different UpdateTimestampsCache-based
			//		invalidation strategies - QueryCacheInvalidationStrategy
		}
		else {
			SQL_EXEC_LOGGER
					.skippingReadingQueryResultCacheData( queryCacheEnabled ? "enabled" : "disabled", cacheMode.name() );
			cachedResults = null;
			queryCacheRegionName = null;
			if ( cacheable && cacheMode.isPutEnabled() ) {
				queryResultsCacheKey = QueryKey.from(
						jdbcSelect.getSqlString(),
						queryOptions.getLimit(),
						executionContext.getQueryParameterBindings(),
						session
				);
			}
			else {
				queryResultsCacheKey = null;
			}
		}

		final ReactiveValuesMappingProducer mappingProducer = (ReactiveValuesMappingProducer) jdbcSelect.getJdbcValuesMappingProducer();
		return resolveJdbcValues( queryIdentifier, executionContext, resultSetAccess, cachedResults, queryResultsCacheKey, mappingProducer )
				.thenApply( jdbcValues -> {
					if ( queryCacheRegionName != null ) {
						final StatisticsImplementor statistics = factory.getStatistics();
						if ( statistics.isStatisticsEnabled() ) {
							if ( jdbcValues.isQueryCacheHit() ) {
								statistics.queryCacheHit( queryIdentifier, queryCacheRegionName );
							}
							else {
								statistics.queryCacheMiss( queryIdentifier, queryCacheRegionName );
							}
						}
					}
					return jdbcValues;
				} );
	}

	private static CompletionStage<ReactiveValuesResultSet> resolveJdbcValues(
			String queryIdentifier,
			ExecutionContext executionContext,
			ReactiveDeferredResultSetAccess resultSetAccess,
			List<?> cachedResults,
			QueryKey queryResultsCacheKey,
			ReactiveValuesMappingProducer mappingProducer) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		final LoadQueryInfluencers loadQueryInfluencers = session.getLoadQueryInfluencers();
		final QueryOptions queryOptions = executionContext.getQueryOptions();

		// Try to use the cached results if available
		return resolveCacheHit( cachedResults, resultSetAccess, mappingProducer, loadQueryInfluencers, factory )
				.thenCompose( cacheHit -> {
					if ( cacheHit != null ) {
						return completedFuture( new ReactiveValuesResultSet( cacheHit, executionContext ) );
					}
					// Execute the query (cache miss or insufficient cached data)
					if ( queryResultsCacheKey == null ) {
						return mappingProducer
								.reactiveResolve( resultSetAccess, loadQueryInfluencers, factory )
								.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet(
										resultSetAccess,
										null,
										queryIdentifier,
										queryOptions,
										false,
										jdbcValuesMapping,
										null,
										executionContext
								) );
					}
					// If we need to put the values into the cache, we need to be able to capture the JdbcValuesMetadata
					final CapturingJdbcValuesMetadata capturingMetadata = new CapturingJdbcValuesMetadata( resultSetAccess );
					return resultSetAccess.getReactiveResultSet()
							.thenCompose( resultSet -> mappingProducer
									.reactiveResolve( capturingMetadata, loadQueryInfluencers, factory ) )
							.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet(
									resultSetAccess,
									queryResultsCacheKey,
									queryIdentifier,
									queryOptions,
									false,
									jdbcValuesMapping,
									capturingMetadata.resolveMetadataForCache( jdbcValuesMapping ),
									executionContext
							) );
				} );
	}

	/**
	 * @return a stage with the {@link JdbcValuesCacheHit}, or with {@code null} if there are no cached results
	 * or if they are not compatible with the result mapping of the query.
	 */
	private static CompletionStage<JdbcValuesCacheHit> resolveCacheHit(
			List<?> cachedResults,
			ReactiveDeferredResultSetAccess resultSetAccess,
			ReactiveValuesMappingProducer mappingProducer,
			LoadQueryInfluencers loadQueryInfluencers,
			SessionFactoryImplementor factory) {
		if ( cachedResults == null ) {
			return nullFuture();
		}
		final JdbcValuesMetadata valuesMetadata =
				!cachedResults.isEmpty() && cachedResults.get( 0 ) instanceof JdbcValuesMetadata jdbcValuesMetadata
						? jdbcValuesMetadata
						: resultSetAccess;
		try {
			return mappingProducer
					.reactiveResolve( valuesMetadata, loadQueryInfluencers, factory )
					.thenApply( resolvedMapping -> new JdbcValuesCacheHit( cachedResults, resolvedMapping ) )
					.handle( (cacheHit, throwable) -> {
						if ( throwable == null ) {
							return cacheHit.isCacheCompatible() ? cacheHit : null;
						}
						// Cached metadata doesn't cover all required columns, the query needs to be re-executed
						return isCacheMetadataIncomplete( throwable ) ? null : rethrow( throwable );
					} );
		}
		catch (CachedJdbcValuesMetadata.CacheMetadataIncompleteException e) {
			return nullFuture();
		}
	}

	private static boolean isCacheMetadataIncomplete(Throwable throwable) {
		return throwable instanceof CachedJdbcValuesMetadata.CacheMetadataIncompleteException
				|| throwable.getCause() instanceof CachedJdbcValuesMetadata.CacheMetadataIncompleteException;
	}

	/**
	 * Copied from Hibernate ORM
	 */
	private static CacheMode resolveCacheMode(ExecutionContext executionContext) {
		final QueryOptions queryOptions = executionContext.getQueryOptions();
		final SharedSessionContract session = executionContext.getSession();
		return coalesceSuppliedValues(
				() -> queryOptions == null ? null : queryOptions.getCacheMode(),
				session::getCacheMode,
				() -> CacheMode.NORMAL
		);
	}

	/**
	 * see {@link CachedJdbcValuesMetadata}
	 */
	public static class CapturingJdbcValuesMetadata implements JdbcValuesMetadata {
		private final ReactiveResultSetAccess resultSetAccess;
		private String[] columnNames;
		private BasicType<?>[] types;

		public CapturingJdbcValuesMetadata(ReactiveResultSetAccess resultSetAccess) {
			this.resultSetAccess = resultSetAccess;
		}

		private void initializeArrays() {
			final int columnCount = resultSetAccess.getColumnCount();
			columnNames = new String[columnCount];
			types = new BasicType[columnCount];
		}

		@Override
		public int getColumnCount() {
			if ( columnNames == null ) {
				initializeArrays();
			}
			return columnNames.length;
		}

		@Override
		public int resolveColumnPosition(String columnName) {
			if ( columnNames == null ) {
				initializeArrays();
			}
			int position;
			if ( columnNames == null ) {
				position = resultSetAccess.resolveColumnPosition( columnName );
				columnNames[position - 1] = columnName;
			}
			else if ( ( position = indexOf( columnNames, columnName ) + 1 ) == 0 ) {
				position = resultSetAccess.resolveColumnPosition( columnName );
				columnNames[position - 1] = columnName;
			}
			return position;
		}

		@Override
		public String resolveColumnName(int position) {
			if ( columnNames == null ) {
				initializeArrays();
			}
			String name;
			if ( columnNames == null ) {
				name = resultSetAccess.resolveColumnName( position );
				columnNames[position - 1] = name;
			}
			else if ( ( name = columnNames[position - 1] ) == null ) {
				name = resultSetAccess.resolveColumnName( position );
				columnNames[position - 1] = name;
			}
			return name;
		}

		@Override
		public <J> BasicType<J> resolveType(
				int position,
				JavaType<J> explicitJavaType,
				TypeConfiguration typeConfiguration) {
			if ( columnNames == null ) {
				initializeArrays();
			}
			final BasicType<J> basicType = resultSetAccess.resolveType(
					position,
					explicitJavaType,
					typeConfiguration
			);
			types[position - 1] = basicType;
			return basicType;
		}

		// FIXME: Review the whole class in ORM, we can probably remove some duplicated code
		public CachedJdbcValuesMetadata resolveMetadataForCache(JdbcValuesMapping jdbcValuesMapping) {
			if ( columnNames == null ) {
				return null;
			}
			// Fill in types from the mapping's SqlSelections for positions that
			// were not captured during mapping resolution (e.g. entity results)
			for ( var selection : jdbcValuesMapping.getSqlSelections() ) {
				final int pos = selection.getValuesArrayPosition();
				if ( types[pos] == null && selection.getExpressionType() != null ) {
					types[pos] = (BasicType<?>) selection.getExpressionType().getSingleJdbcMapping();
				}
			}
			return new CachedJdbcValuesMetadata( columnNames, types, jdbcValuesMapping.getValueIndexesToCacheIndexes() );
		}
	}

	private static class Statistics {
		private final ExecutionContext executionContext;

		private final StatisticsImplementor statistics;

		private final boolean enabled;
		private long startTime = 0;

		public Statistics(ExecutionContext executionContext, ReactiveValuesResultSet jdbcValues) {
			this.executionContext = executionContext;
			statistics = executionContext.getSession().getFactory().getStatistics();
			if ( executionContext.hasQueryExecutionToBeAddedToStatistics()
					// FIXME: Add this check back later
					//  && jdbcValues instanceof JdbcValuesResultSetImpl
			) {
				enabled = statistics.isStatisticsEnabled();
				if ( enabled ) {
					startTime = System.nanoTime();
				}
			}
			else {
				enabled = false;
			}
		}

		public <T> void end(JdbcSelect jdbcSelect, T result) {
			if ( enabled ) {
				final long endTime = System.nanoTime();
				final long milliseconds = TimeUnit.MILLISECONDS
						.convert( endTime - startTime, TimeUnit.NANOSECONDS );
				statistics.queryExecuted(
						executionContext.getQueryIdentifier( jdbcSelect.getSqlString() ),
						getResultSize( result ),
						milliseconds
				);
			}
		}

		private <T> int getResultSize(T result) {
			return result instanceof Collection<?> collection ? collection.size() : -1;
		}
	}
}

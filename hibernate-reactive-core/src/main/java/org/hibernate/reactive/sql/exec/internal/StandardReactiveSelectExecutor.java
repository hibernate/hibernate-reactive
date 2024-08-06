/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.hibernate.CacheMode;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.query.TupleTransformer;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
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
import org.hibernate.sql.exec.SqlExecLogger;
import org.hibernate.sql.exec.internal.JdbcExecHelper;
import org.hibernate.sql.exec.internal.StandardStatementCreator;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelectExecutor;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;
import org.hibernate.sql.results.internal.RowTransformerTupleTransformerAdapter;
import org.hibernate.sql.results.jdbc.internal.CachedJdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.RowTransformer;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * @see org.hibernate.sql.exec.internal.JdbcSelectExecutorStandardImpl
 */
public class StandardReactiveSelectExecutor implements ReactiveSelectExecutor {

	public static final StandardReactiveSelectExecutor INSTANCE = new StandardReactiveSelectExecutor();

	private StandardReactiveSelectExecutor() {
	}

	public <R> CompletionStage<List<R>> list(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic) {
		return list( jdbcSelect, jdbcParameterBindings, executionContext, rowTransformer, null, uniqueSemantic );
	}

	public <R> CompletionStage<List<R>> list(
			JdbcOperationQuerySelect jdbcSelect,
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
			JdbcOperationQuerySelect jdbcSelect,
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

	@Override
	public <T, R> CompletionStage<T> executeQuery(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			int resultCountEstimate,
			JdbcSelectExecutor.StatementCreator statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer) {

		final PersistenceContext persistenceContext = executionContext.getSession().getPersistenceContext();
		boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
		Boolean readOnly = executionContext.getQueryOptions().isReadOnly();
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
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> transformer,
			Class<R> domainResultType,
			int resultCountEstimate,
			JdbcSelectExecutor.StatementCreator statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer) {

		final ReactiveDeferredResultSetAccess deferredResultSetAccess = new ReactiveDeferredResultSetAccess( jdbcSelect, jdbcParameterBindings, executionContext, statementCreator, resultCountEstimate );

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

					final JdbcValuesSourceProcessingStateStandardImpl valuesProcessingState = new JdbcValuesSourceProcessingStateStandardImpl(
							executionContext,
							processingOptions
					);

					final ReactiveRowReader<R> rowReader = ReactiveResultsHelper.createRowReader(
							executionContext.getSession().getSessionFactory(),
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

					return resultsConsumer
							.consume(
									jdbcValues,
									executionContext.getSession(),
									processingOptions,
									valuesProcessingState,
									rowProcessingState,
									rowReader
							)
							.thenApply( result -> {
								statistics.end( jdbcSelect, result );
								return result;
							} );
				} );
	}

	private static <R> RowTransformer<R> rowTransformer(
			ExecutionContext executionContext,
			RowTransformer<R> transformer,
			ReactiveValuesResultSet jdbcValues) {
		RowTransformer<R> rowTransformer = transformer;
		if ( rowTransformer == null ) {
			@SuppressWarnings("unchecked") final TupleTransformer<R> tupleTransformer = (TupleTransformer<R>) executionContext
					.getQueryOptions()
					.getTupleTransformer();

			if ( tupleTransformer == null ) {
				rowTransformer = RowTransformerStandardImpl.instance();
			}
			else {
				final List<DomainResult<?>> domainResults = jdbcValues.getValuesMapping()
						.getDomainResults();
				final String[] aliases = new String[domainResults.size()];
				for ( int i = 0; i < domainResults.size(); i++ ) {
					aliases[i] = domainResults.get( i ).getResultVariable();
				}
				rowTransformer = new RowTransformerTupleTransformerAdapter<>( aliases, tupleTransformer );
			}
		}
		return rowTransformer;
	}

	public CompletionStage<ReactiveValuesResultSet> resolveJdbcValuesSource(
			String queryIdentifier,
			JdbcOperationQuerySelect jdbcSelect,
			boolean canBeCached,
			ExecutionContext executionContext,
			ReactiveDeferredResultSetAccess resultSetAccess) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		final boolean queryCacheEnabled = factory.getSessionFactoryOptions().isQueryCacheEnabled();

		final List<?> cachedResults;
		final CacheMode cacheMode = JdbcExecHelper.resolveCacheMode( executionContext );
		final boolean cacheable = queryCacheEnabled
				&& canBeCached
				&& executionContext.getQueryOptions().isResultCachingEnabled() == Boolean.TRUE;
		final QueryKey queryResultsCacheKey;

		if ( cacheable && cacheMode.isGetEnabled() ) {
			SqlExecLogger.SQL_EXEC_LOGGER.debugf( "Reading Query result cache data per CacheMode#isGetEnabled [%s]", cacheMode.name() );
			final Set<String> querySpaces = jdbcSelect.getAffectedTableNames();
			if ( querySpaces == null || querySpaces.isEmpty() ) {
				SqlExecLogger.SQL_EXEC_LOGGER.tracef( "Unexpected querySpaces is empty" );
			}
			else {
				SqlExecLogger.SQL_EXEC_LOGGER.tracef( "querySpaces is `%s`", querySpaces );
			}

			final QueryResultsCache queryCache = factory.getCache()
					.getQueryResultsCache( executionContext.getQueryOptions().getResultCacheRegionName() );

			queryResultsCacheKey = QueryKey
					.from( jdbcSelect.getSqlString(), executionContext.getQueryOptions().getLimit(), executionContext.getQueryParameterBindings(), session );

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

			final StatisticsImplementor statistics = factory.getStatistics();
			if ( statistics.isStatisticsEnabled() ) {
				if ( cachedResults == null ) {
					statistics.queryCacheMiss( queryIdentifier, queryCache.getRegion().getName() );
				}
				else {
					statistics.queryCacheHit( queryIdentifier, queryCache.getRegion().getName() );
				}
			}
		}
		else {
			SqlExecLogger.SQL_EXEC_LOGGER
					.debugf( "Skipping reading Query result cache data: cache-enabled = %s, cache-mode = %s", queryCacheEnabled, cacheMode.name() );
			cachedResults = null;
			if ( cacheable && cacheMode.isPutEnabled() ) {
				queryResultsCacheKey = QueryKey.from(
						jdbcSelect.getSqlString(),
						executionContext.getQueryOptions().getLimit(),
						executionContext.getQueryParameterBindings(),
						session
				);
			}
			else {
				queryResultsCacheKey = null;
			}
		}

		ReactiveValuesMappingProducer mappingProducer = (ReactiveValuesMappingProducer) jdbcSelect.getJdbcValuesMappingProducer();
		if ( cachedResults == null ) {
			if ( queryResultsCacheKey == null ) {
				return mappingProducer
						.reactiveResolve( resultSetAccess, session.getLoadQueryInfluencers(), factory )
						.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet(
								resultSetAccess,
								null,
								queryIdentifier,
								executionContext.getQueryOptions(),
								resultSetAccess.usesFollowOnLocking(),
								jdbcValuesMapping,
								null,
								executionContext
						) );
			}
			else {
				// If we need to put the values into the cache, we need to be able to capture the JdbcValuesMetadata
				final CapturingJdbcValuesMetadata capturingMetadata = new CapturingJdbcValuesMetadata( resultSetAccess );
				return mappingProducer
						.reactiveResolve( resultSetAccess, session.getLoadQueryInfluencers(), factory )
						.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet(
								resultSetAccess,
								queryResultsCacheKey,
								queryIdentifier,
								executionContext.getQueryOptions(),
								resultSetAccess.usesFollowOnLocking(),
								jdbcValuesMapping,
								capturingMetadata.resolveMetadataForCache(),
								executionContext
						) );
			}
		}
		else {
			// TODO: Implements JdbcValuesCacheHit for reactive, see JdbcSelectExecutorStandardImpl#resolveJdbcValuesSource
			// If we need to put the values into the cache, we need to be able to capture the JdbcValuesMetadata
			final CapturingJdbcValuesMetadata capturingMetadata = new CapturingJdbcValuesMetadata( resultSetAccess );
			if ( cachedResults.isEmpty() || !( cachedResults.get( 0 ) instanceof JdbcValuesMetadata ) ) {
				return mappingProducer.reactiveResolve( resultSetAccess, session.getLoadQueryInfluencers(), factory )
						.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet(
								resultSetAccess,
								queryResultsCacheKey,
								queryIdentifier,
								executionContext.getQueryOptions(),
								resultSetAccess.usesFollowOnLocking(),
								jdbcValuesMapping,
								capturingMetadata.resolveMetadataForCache(),
								executionContext
						) );
			}
			else {
				return mappingProducer
						.reactiveResolve( (JdbcValuesMetadata) cachedResults.get( 0 ), session.getLoadQueryInfluencers(), factory )
						.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet(
								resultSetAccess,
								queryResultsCacheKey,
								queryIdentifier,
								executionContext.getQueryOptions(),
								resultSetAccess.usesFollowOnLocking(),
								jdbcValuesMapping,
								capturingMetadata.resolveMetadataForCache(),
								executionContext
						) );
			}
		}
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
			else if ( ( position = ArrayHelper.indexOf( columnNames, columnName ) + 1 ) == 0 ) {
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

		public CachedJdbcValuesMetadata resolveMetadataForCache() {
			if ( columnNames == null ) {
				return null;
			}
			return new CachedJdbcValuesMetadata( columnNames, types );
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

		public <T> void end(JdbcOperationQuerySelect jdbcSelect, T result) {
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
			return result instanceof Collection
					? ( (Collection<?>) result ).size()
					: -1;
		}
	}
}

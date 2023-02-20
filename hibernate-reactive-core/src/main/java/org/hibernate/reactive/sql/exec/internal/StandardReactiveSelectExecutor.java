/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.hibernate.CacheMode;
import org.hibernate.LockOptions;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.query.TupleTransformer;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveSelectExecutor;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.reactive.sql.results.internal.ReactiveDeferredResultSetAccess;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveValuesMappingProducer;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.exec.SqlExecLogger;
import org.hibernate.sql.exec.internal.JdbcExecHelper;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.internal.ResultsHelper;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;
import org.hibernate.sql.results.internal.RowTransformerTupleTransformerAdapter;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.RowReader;
import org.hibernate.sql.results.spi.RowTransformer;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * @see org.hibernate.sql.exec.internal.JdbcSelectExecutorStandardImpl
 */
public class StandardReactiveSelectExecutor implements ReactiveSelectExecutor {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

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
		return executeQuery(
				jdbcSelect,
				jdbcParameterBindings,
				executionContext,
				rowTransformer,
				domainResultType,
				sql -> executionContext.getSession()
						.getJdbcCoordinator()
						.getStatementPreparer()
						.prepareStatement( sql ),
				ReactiveListResultsConsumer.instance( uniqueSemantic )
		);
	}

	@Override
	public <T, R> CompletionStage<T> executeQuery(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultType,
			Function<String, PreparedStatement> statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer) {

		final PersistenceContext persistenceContext = executionContext.getSession().getPersistenceContext();
		boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
		Boolean readOnly = executionContext.getQueryOptions().isReadOnly();
		if ( readOnly != null ) {
			// The read-only/modifiable mode for the query was explicitly set.
			// Temporarily set the default read-only/modifiable setting to the query's setting.
			persistenceContext.setDefaultReadOnly( readOnly );
		}

		return doExecuteQuery( jdbcSelect, jdbcParameterBindings, executionContext, rowTransformer, domainResultType, statementCreator, resultsConsumer )
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
			Function<String, PreparedStatement> statementCreator,
			ReactiveResultsConsumer<T, R> resultsConsumer) {

		final ReactiveDeferredResultSetAccess deferredResultSetAccess = new ReactiveDeferredResultSetAccess( jdbcSelect, jdbcParameterBindings, executionContext, statementCreator );

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
							processingOptions,
							executionContext::registerLoadingEntityEntry
					);

					final RowReader<R> rowReader = ResultsHelper.createRowReader(
							executionContext,
							// If follow-on locking is used, we must omit the lock options here,
							// because these lock options are only for Initializers.
							// If we wouldn't omit this, the follow-on lock requests would be no-ops,
							// because the EntityEntries would already have the desired lock mode
							deferredResultSetAccess.usesFollowOnLocking()
									? LockOptions.NONE
									: executionContext.getQueryOptions().getLockOptions(),
							rowTransformer,
							domainResultType,
							jdbcValues.getValuesMapping()
					);

					final ReactiveRowProcessingState rowProcessingState = new ReactiveRowProcessingState(
							valuesProcessingState,
							executionContext,
							rowReader,
							jdbcValues
					);

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

	public CompletionStage<ReactiveValuesResultSet> resolveJdbcValuesSource(String queryIdentifier, JdbcOperationQuerySelect jdbcSelect, boolean canBeCached, ExecutionContext executionContext, ReactiveResultSetAccess resultSetAccess) {
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
			if ( querySpaces == null || querySpaces.size() == 0 ) {
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
						.reactiveResolve( resultSetAccess, factory )
						.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet( resultSetAccess, null, queryIdentifier, executionContext.getQueryOptions(), jdbcValuesMapping, null, executionContext ) );
			}
			else {
				// If we need to put the values into the cache, we need to be able to capture the JdbcValuesMetadata
				final CapturingJdbcValuesMetadata capturingMetadata = new CapturingJdbcValuesMetadata( resultSetAccess );
				JdbcValuesMetadata metadataForCache = capturingMetadata.resolveMetadataForCache();
				return mappingProducer
						.reactiveResolve( resultSetAccess, factory )
						.thenApply( jdbcValuesMapping -> new ReactiveValuesResultSet( resultSetAccess, queryResultsCacheKey, queryIdentifier, executionContext.getQueryOptions(), jdbcValuesMapping, metadataForCache, executionContext ) );
			}
		}
		else {
			// If we need to put the values into the cache, we need to be able to capture the JdbcValuesMetadata
			final CapturingJdbcValuesMetadata capturingMetadata = new CapturingJdbcValuesMetadata( resultSetAccess );
			CompletionStage<JdbcValuesMapping> stage = CompletionStages.nullFuture();
			if ( cachedResults.isEmpty() || !( cachedResults.get( 0 ) instanceof JdbcValuesMetadata ) ) {
				stage = stage.thenCompose(v -> mappingProducer.reactiveResolve(resultSetAccess, factory));
			}
			else {
				stage = stage.thenCompose(v -> mappingProducer.reactiveResolve( (JdbcValuesMetadata) cachedResults.get( 0 ), factory ));
			}
			return stage.thenApply(jdbcValuesMapping -> new ReactiveValuesResultSet( resultSetAccess, queryResultsCacheKey, queryIdentifier, executionContext.getQueryOptions(), jdbcValuesMapping, capturingMetadata, executionContext));
		}
	}

	/**
	 * see {@link org.hibernate.sql.exec.internal.JdbcSelectExecutorStandardImpl.CapturingJdbcValuesMetadata}
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

		public JdbcValuesMetadata resolveMetadataForCache() {
			if ( columnNames == null ) {
				return null;
			}
			return new CachedJdbcValuesMetadata( columnNames, types );
		}
	}

	private static class CachedJdbcValuesMetadata implements JdbcValuesMetadata, Serializable {
		private final String[] columnNames;
		private final BasicType<?>[] types;

		public CachedJdbcValuesMetadata(String[] columnNames, BasicType<?>[] types) {
			this.columnNames = columnNames;
			this.types = types;
		}

		@Override
		public int getColumnCount() {
			return columnNames.length;
		}

		@Override
		public int resolveColumnPosition(String columnName) {
			final int position = ArrayHelper.indexOf( columnNames, columnName ) + 1;
			if ( position == 0 ) {
				throw new IllegalStateException( "Unexpected resolving of unavailable column: " + columnName );
			}
			return position;
		}

		@Override
		public String resolveColumnName(int position) {
			final String name = columnNames[position - 1];
			if ( name == null ) {
				throw new IllegalStateException( "Unexpected resolving of unavailable column at position: " + position );
			}
			return name;
		}

		@Override
		public <J> BasicType<J> resolveType(
				int position,
				JavaType<J> explicitJavaType,
				TypeConfiguration typeConfiguration) {
			final BasicType<?> type = types[position - 1];
			if ( type == null ) {
				throw new IllegalStateException( "Unexpected resolving of unavailable column at position: " + position );
			}
			if ( explicitJavaType == null || type.getJavaTypeDescriptor() == explicitJavaType ) {
				//noinspection unchecked
				return (BasicType<J>) type;
			}
			else {
				return typeConfiguration.getBasicTypeRegistry().resolve(
						explicitJavaType,
						type.getJdbcType()
				);
			}
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

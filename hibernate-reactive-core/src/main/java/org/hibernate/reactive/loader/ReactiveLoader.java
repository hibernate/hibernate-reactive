package org.hibernate.reactive.loader;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.FilterKey;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.dialect.pagination.NoopLimitHandler;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.transform.CacheableResultTransformer;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor.toParameterArray;

/**
 * Defines common reactive operations inherited by all kinds of loaders.
 *
 * @see org.hibernate.loader.Loader
 *
 * @author Gavin King
 */
public interface ReactiveLoader {

	/**
	 * Process query string by applying filters, LIMIT clause, locks and comments if necessary.
	 * Finally execute SQL statement and advance to the first row.
	 */
	default CompletionStage<List<Object>> executeReactiveQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SessionImplementor session,
			Function<ResultSet, List<Object>> transformer) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = limitHandler( queryParameters.getRowSelection() );
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		return session.unwrap(ReactiveSession.class)
				.getReactiveConnection()
				.selectJdbc( sql, toParameterArray(queryParameters, session) )
				.thenApply( transformer );
	}

	default CompletionStage<List<Object>> doReactiveQuery(
			String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		return executeReactiveQueryStatement(
				sql,
				queryParameters,
				false,
				afterLoadActions,
				session,
				resultSet -> {
					try {
						if ( queryParameters.hasAutoDiscoverScalarTypes() ) {
							autoDiscoverTypes( resultSet );
						}
						return processResultSet(
								resultSet,
								queryParameters,
								session,
								returnProxies,
								forcedResultTransformer,
								maxRows,
								afterLoadActions
						);
					}
					catch (SQLException sqle) {
						//don't log or convert it - just pass it on to the caller
						throw new JDBCException( "could not load batch", sqle );
					}
				}
		);
	}

	default CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters) {
		return doReactiveQueryAndInitializeNonLazyCollections(sql, session, queryParameters, false, null);
	}

	default CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
		if ( queryParameters.isReadOnlyInitialized() ) {
			// The read-only/modifiable mode for the query was explicitly set.
			// Temporarily set the default read-only/modifiable setting to the query's setting.
			persistenceContext.setDefaultReadOnly( queryParameters.isReadOnly() );
		}
		else {
			// The read-only/modifiable setting for the query was not initialized.
			// Use the default read-only/modifiable from the persistence context instead.
			queryParameters.setReadOnly( persistenceContext.isDefaultReadOnly() );
		}
		persistenceContext.beforeLoad();
		return doReactiveQuery( sql, session, queryParameters, returnProxies, forcedResultTransformer )
				.whenComplete( (list, e) -> persistenceContext.afterLoad() )
				.thenCompose( list ->
						// only initialize non-lazy collections after everything else has been refreshed
						((ReactivePersistenceContextAdapter) persistenceContext ).reactiveInitializeNonLazyCollections()
								.thenApply(v -> list)
				)
				.whenComplete( (list, e) -> persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig) );
	}

	default CompletionStage<List<Object>> doReactiveList(
			final String sql, final String queryIdentifier,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException {

		final StatisticsImplementor statistics = getFactory().getStatistics();
		final boolean stats = statistics.isStatisticsEnabled();
		final long startTime = stats ? System.nanoTime() : 0;

		return doReactiveQueryAndInitializeNonLazyCollections( sql, session, queryParameters, true, forcedResultTransformer )
				.handle( (list, e ) -> {
					if ( e instanceof SQLException ) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper()
								.convert( (SQLException) e, "could not execute query", sql );
					}
					else if ( e != null ) {
						CompletionStages.rethrow( e );
					}

					if ( stats ) {
						final long endTime = System.nanoTime();
						final long milliseconds = TimeUnit.MILLISECONDS.convert( endTime - startTime, TimeUnit.NANOSECONDS );
						statistics.queryExecuted( queryIdentifier, list.size(), milliseconds );
					}
					return list;
				} );
	}

	default CompletionStage<List<Object>> reactiveListIgnoreQueryCache(
			String sql, String queryIdentifier,
			SharedSessionContractImplementor session,
			QueryParameters queryParameters) {
		return doReactiveList( sql, queryIdentifier, (SessionImplementor) session, queryParameters, null )
				.thenApply( result -> getResultList( result, queryParameters.getResultTransformer() ) );
	}

	default CompletionStage<List<Object>> reactiveListUsingQueryCache(
			final String sql, final String queryIdentifier,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final Set<Serializable> querySpaces,
			final Type[] resultTypes) {

		QueryResultsCache queryCache = getFactory().getCache().getQueryResultsCache( queryParameters.getCacheRegion() );

		QueryKey key = queryKey( sql, session, queryParameters );

//		if ( querySpaces == null || querySpaces.size() == 0 ) {
//			LOG.tracev( "Unexpected querySpaces is {0}", ( querySpaces == null ? querySpaces : "empty" ) );
//		}
//		else {
//			LOG.tracev( "querySpaces is {0}", querySpaces );
//		}

		List<Object> resultFromCache = getResultFromQueryCache( session, queryParameters, querySpaces, resultTypes, queryCache, key );
		CompletionStage<List<Object>> resultStage = CompletionStages.completedFuture( resultFromCache )
				.thenCompose( result -> {
					if ( result == null ) {
						return doReactiveList( sql, queryIdentifier, session, queryParameters, key.getResultTransformer() )
								.thenApply( cachableList -> {
									putResultInQueryCache( session, queryParameters, resultTypes, queryCache, key, cachableList );
									return cachableList;
								} );
					}
					return CompletionStages.completedFuture( result );
				} );

		return resultStage.thenApply( result -> {
			ResultTransformer resolvedTransformer = resolveResultTransformer( queryParameters.getResultTransformer() );
			List<?> resultList;
			if (resolvedTransformer == null) {
				resultList = result;
			}
			else {
				resultList = areResultSetRowsTransformedImmediately()
						? key.getResultTransformer().retransformResults( result, getResultRowAliases(), queryParameters.getResultTransformer(), includeInResultRow() )
						: key.getResultTransformer().untransformToTuples( result );
			}
			return getResultList( resultList, queryParameters.getResultTransformer() );
		} );
	}

	default QueryKey queryKey(String sql, SessionImplementor session, QueryParameters queryParameters) {
		return QueryKey.generateQueryKey(
				sql,
				queryParameters,
				FilterKey.createFilterKeys( session.getLoadQueryInfluencers().getEnabledFilters() ),
				session,
				cacheableResultTransformer( queryParameters )
		);
	}

	default CacheableResultTransformer cacheableResultTransformer(QueryParameters queryParameters) {
		return CacheableResultTransformer.create(
				queryParameters.getResultTransformer(),
				getResultRowAliases(),
				includeInResultRow()
		);
	}

	default LimitHandler limitHandler(RowSelection selection) {
		final LimitHandler limitHandler = getFactory().getDialect().getLimitHandler();
		return LimitHelper.useLimit( limitHandler, selection ) ? limitHandler : NoopLimitHandler.INSTANCE;
	}

	boolean[] includeInResultRow();

	List<Object> getResultFromQueryCache(SessionImplementor session, QueryParameters queryParameters, Set<Serializable> querySpaces, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key);

	void putResultInQueryCache(SessionImplementor session, QueryParameters queryParameters, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key, List<Object> cachableList);

	ResultTransformer resolveResultTransformer(ResultTransformer resultTransformer);

	String[] getResultRowAliases();

	boolean areResultSetRowsTransformedImmediately();

	List<Object> getResultList(List<?> results, ResultTransformer resultTransformer) throws QueryException;

	List<Object> processResultSet(
			ResultSet rs,
			QueryParameters queryParameters,
			SharedSessionContractImplementor session,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer,
			int maxRows,
			List<AfterLoadAction> afterLoadActions) throws SQLException;

	String preprocessSQL(String sql, QueryParameters queryParameters, SessionFactoryImplementor factory,
						 List<AfterLoadAction> afterLoadActions);

	SessionFactoryImplementor getFactory();

	void autoDiscoverTypes(ResultSet rs);

}

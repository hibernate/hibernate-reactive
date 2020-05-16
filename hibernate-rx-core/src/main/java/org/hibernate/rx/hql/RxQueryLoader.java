package org.hibernate.rx.hql;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.hibernate.HibernateException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.ast.QueryTranslatorImpl;
import org.hibernate.hql.internal.ast.tree.SelectClause;
import org.hibernate.loader.hql.QueryLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

public class RxQueryLoader extends QueryLoader {

	private final QueryTranslatorImpl queryTranslator;
	private final SessionFactoryImplementor factory;
	private final SelectClause selectClause;

	public RxQueryLoader(
			QueryTranslatorImpl queryTranslator,
			SessionFactoryImplementor factory,
			SelectClause selectClause) {
		super( queryTranslator, factory, selectClause );
		this.queryTranslator = queryTranslator;
		this.factory = factory;
		this.selectClause = selectClause;
	}

	public CompletionStage<List<Object>> rxList(
			SessionImplementor session,
			QueryParameters queryParameters) throws HibernateException {
		checkQuery( queryParameters );
		return rxList( session, queryParameters, queryTranslator.getQuerySpaces(), selectClause.getQueryReturnTypes() );
	}

	/**
	 * Return the query results, using the query cache, called
	 * by subclasses that implement cacheable queries
	 * @see QueryLoader#list(SharedSessionContractImplementor, QueryParameters, Set, Type[]) 
	 */
	protected CompletionStage<List<Object>> rxList(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final Set<Serializable> querySpaces,
			final Type[] resultTypes) throws HibernateException {
		final boolean cacheable = factory.getSessionFactoryOptions().isQueryCacheEnabled() &&
				queryParameters.isCacheable();

		if ( cacheable ) {
			return listUsingQueryCache( session, queryParameters, querySpaces, resultTypes );
		}
		else {
			return listIgnoreQueryCache( session, queryParameters );
		}
	}

	private CompletionStage<List<Object>> listIgnoreQueryCache(SharedSessionContractImplementor session, QueryParameters queryParameters) {
		return doRxList( (SessionImplementor) session, queryParameters, null )
				.thenApply( result -> getResultList( result, queryParameters.getResultTransformer() ) );
	}

	@Override @SuppressWarnings("unchecked")
	protected List<Object> getResultList(List results, ResultTransformer resultTransformer) throws QueryException {
		return super.getResultList(results, resultTransformer);
	}

	private CompletionStage<List<Object>> listUsingQueryCache(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final Set<Serializable> querySpaces,
			final Type[] resultTypes) {

		QueryResultsCache queryCache = factory.getCache().getQueryResultsCache( queryParameters.getCacheRegion() );

		QueryKey key = generateQueryKey( session, queryParameters );

		if ( querySpaces == null || querySpaces.size() == 0 ) {
			LOG.tracev( "Unexpected querySpaces is {0}", ( querySpaces == null ? querySpaces : "empty" ) );
		}
		else {
			LOG.tracev( "querySpaces is {0}", querySpaces );
		}

		List<Object> resultFromCache = getResultFromQueryCache( session, queryParameters, querySpaces, resultTypes, queryCache, key );
		CompletionStage<List<Object>> resultStage = RxUtil.completedFuture( resultFromCache )
				.thenCompose( result -> {
					if ( result == null ) {
						return doRxList( session, queryParameters, key.getResultTransformer() )
								.thenApply( cachableList -> {
									putResultInQueryCache( session, queryParameters, resultTypes, queryCache, key, cachableList );
									return cachableList;
								} );
					}
					return RxUtil.completedFuture( result );
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
						: key.getResultTransformer().untransformToTuples( result) ;
			}
			return getResultList( resultList, queryParameters.getResultTransformer() );
		} );
	}

	/**
	 * @see QueryLoader#doList(SharedSessionContractImplementor, QueryParameters, ResultTransformer)
	 */
	private CompletionStage<List<Object>> doRxList(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException {

		final StatisticsImplementor statistics = getFactory().getStatistics();
		final boolean stats = statistics.isStatisticsEnabled();
		final long startTime = stats ? System.nanoTime() : 0;

		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, true, forcedResultTransformer )
				.handle( (list, e ) -> {
					if ( e instanceof SQLException ) {
						throw factory.getJdbcServices().getSqlExceptionHelper()
								.convert( (SQLException) e, "could not execute query", getSQLString() );
					}
					else if ( e != null ) {
						RxUtil.rethrow( e );
					}

					if ( stats ) {
						final long endTime = System.nanoTime();
						final long milliseconds = TimeUnit.MILLISECONDS.convert( endTime - startTime, TimeUnit.NANOSECONDS );
						statistics.queryExecuted( getQueryIdentifier(), list.size(), milliseconds );
					}
					return list;
				} );
	}

	public CompletionStage<List<Object>> doRxQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException {
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
		return doRxQuery( session, queryParameters, returnProxies, forcedResultTransformer )
				.handle( (list, e) -> {
					persistenceContext.afterLoad();
					if ( e == null ) {
						persistenceContext.initializeNonLazyCollections();
					}
					persistenceContext.setDefaultReadOnly( defaultReadOnlyOrig );
					if ( e != null ) {
						RxUtil.rethrow( e );
					}
					return list;
				} );
	}

	private CompletionStage<List<Object>> doRxQuery(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection )
				? selection.getMaxRows()
				: Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		return executeRxQueryStatement( getSQLString(), queryParameters, false, afterLoadActions, session,
				resultSet -> {
					try {
						return processResultSet( resultSet, queryParameters, session, returnProxies,
								forcedResultTransformer, maxRows, afterLoadActions );
					}
					catch (SQLException sqle) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								sqle,
								"could not load an entity batch: " + MessageHelper.infoString(
										getEntityPersisters()[0],
										queryParameters.getOptionalId(),
										session.getFactory()
								),
								getSQLString()
						);
					}
				}
		);
	}

	/**
	 * Process query string by applying filters, LIMIT clause, locks and comments if necessary.
	 * Finally execute SQL statement and advance to the first row.
	 */
	protected CompletionStage<List<Object>> executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SessionImplementor session,
			Function<ResultSet, List<Object>> transformer) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler( queryParameters.getRowSelection() );
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		return new RxQueryExecutor().execute( sql, queryParameters, session, transformer );
	}
}

package org.hibernate.reactive.session.impl;

import org.hibernate.HibernateException;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.custom.CustomLoader;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.transform.ResultTransformer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor.toParameterArray;

/**
 * @author Gavin King
 */
public class ReactiveCustomLoader extends CustomLoader {
	public ReactiveCustomLoader(CustomQuery customQuery, SessionFactoryImplementor factory) {
		super(customQuery, factory);
	}

	public <T> CompletionStage<List<T>> reactiveList(SharedSessionContractImplementor session, QueryParameters queryParameters) throws HibernateException {
		return listIgnoreQueryCache( session, queryParameters );
	}

	private <T> CompletionStage<List<T>> listIgnoreQueryCache(SharedSessionContractImplementor session, QueryParameters queryParameters) {
		return doReactiveList( (SessionImplementor) session, queryParameters, null )
				.thenApply( result -> getResultList( result, queryParameters.getResultTransformer() ) );
	}

	/**
	 * @see org.hibernate.loader.Loader#doList(SharedSessionContractImplementor, QueryParameters, ResultTransformer)
	 */
	private CompletionStage<List<Object>> doReactiveList(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException {

		final StatisticsImplementor statistics = getFactory().getStatistics();
		final boolean stats = statistics.isStatisticsEnabled();
		final long startTime = stats ? System.nanoTime() : 0;

		return doReactiveQueryAndInitializeNonLazyCollections( session, queryParameters, true, forcedResultTransformer )
				.handle( (list, e ) -> {
					if ( e instanceof SQLException) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper()
								.convert( (SQLException) e, "could not execute query", getSQLString() );
					}
					else if ( e != null ) {
						CompletionStages.rethrow( e );
					}

					if ( stats ) {
						final long endTime = System.nanoTime();
						final long milliseconds = TimeUnit.MILLISECONDS.convert( endTime - startTime, TimeUnit.NANOSECONDS );
						statistics.queryExecuted( getQueryIdentifier(), list.size(), milliseconds );
					}
					return list;
				} );
	}

	/**
	 * dupe of {@link org.hibernate.reactive.loader.ReactiveOuterJoinLoader#doReactiveQueryAndInitializeNonLazyCollections}
	 */
	public CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
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
		return doReactiveQuery( session, queryParameters, returnProxies, forcedResultTransformer )
				.whenComplete( (list, e) -> persistenceContext.afterLoad() )
				.thenCompose( list ->
						// only initialize non-lazy collections after everything else has been refreshed
						((ReactivePersistenceContextAdapter) persistenceContext ).reactiveInitializeNonLazyCollections()
								.thenApply(v -> list)
				)
				.whenComplete( (list, e) -> persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig) );
	}

	/**
	 * dupe of {@link org.hibernate.reactive.loader.ReactiveOuterJoinLoader#doReactiveQuery}
	 */
	private CompletionStage<List<Object>> doReactiveQuery(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection )
				? selection.getMaxRows()
				: Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		return executeReactiveQueryStatement( getSQLString(), queryParameters, false, afterLoadActions, session,
				resultSet -> {
					try {
						autoDiscoverTypes( resultSet );
						return processResultSet( resultSet, queryParameters, session, returnProxies,
								forcedResultTransformer, maxRows, afterLoadActions );
					}
					catch (SQLException sqle) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								sqle,
								"could not execute query",
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
	protected CompletionStage<List<Object>> executeReactiveQueryStatement(
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

		return session.unwrap(ReactiveSession.class)
				.getReactiveConnection()
				.selectJdbc( sql, toParameterArray(queryParameters, session) )
				.thenApply( transformer );
	}
}

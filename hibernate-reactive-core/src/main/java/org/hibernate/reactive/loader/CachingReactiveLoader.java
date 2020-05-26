/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader;

import org.hibernate.HibernateException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.FilterKey;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.transform.CacheableResultTransformer;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Defines common reactive operations inherited by query loaders, in
 * particular, interaction with the cache.
 *
 * @see org.hibernate.loader.Loader
 *
 * @author Gavin King
 */
public interface CachingReactiveLoader extends ReactiveLoader {

	default CompletionStage<List<Object>> doReactiveList(
			final String sql, final String queryIdentifier,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException {

		final StatisticsImplementor statistics = session.getSessionFactory().getStatistics();
		final boolean stats = statistics.isStatisticsEnabled();
		final long startTime = stats ? System.nanoTime() : 0;

		return doReactiveQueryAndInitializeNonLazyCollections( sql, session, queryParameters, true, forcedResultTransformer )
				.handle( (list, e ) -> {
					if ( e instanceof SQLException ) {
						throw session.getSessionFactory().getJdbcServices().getSqlExceptionHelper()
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

		QueryResultsCache queryCache = session.getSessionFactory().getCache()
				.getQueryResultsCache( queryParameters.getCacheRegion() );

		QueryKey key = queryKey( sql, session, queryParameters );

		List<Object> cachedList = getResultFromQueryCache( session, queryParameters, querySpaces, resultTypes, queryCache, key );

		CompletionStage<List<Object>> list;
		if ( cachedList == null ) {
			list = doReactiveList( sql, queryIdentifier, session, queryParameters, key.getResultTransformer() )
					.thenApply( cachableList -> {
						putResultInQueryCache( session, queryParameters, resultTypes, queryCache, key, cachableList );
						return cachableList;
					} );
		}
		else {
			list = CompletionStages.completedFuture( cachedList );
		}

		return list.thenApply(
				result -> getResultList(
						transform( queryParameters, key, result,
								resolveResultTransformer( queryParameters.getResultTransformer() ) ),
						queryParameters.getResultTransformer()
				)
		);
	}

	default List<?> transform(QueryParameters queryParameters, QueryKey key, List<Object> result,
							  ResultTransformer resolvedTransformer) {
		if (resolvedTransformer == null) {
			return result;
		}
		else {
			CacheableResultTransformer transformer = key.getResultTransformer();
			if ( areResultSetRowsTransformedImmediately() ) {
				return transformer.retransformResults(
						result,
						getResultRowAliases(),
						queryParameters.getResultTransformer(),
						includeInResultRow()
				);
			}
			else {
				return transformer.untransformToTuples(result);
			}
		}
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

	boolean[] includeInResultRow();

	List<Object> getResultFromQueryCache(SessionImplementor session, QueryParameters queryParameters, Set<Serializable> querySpaces, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key);

	void putResultInQueryCache(SessionImplementor session, QueryParameters queryParameters, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key, List<Object> cachableList);

	ResultTransformer resolveResultTransformer(ResultTransformer resultTransformer);

	String[] getResultRowAliases();

	boolean areResultSetRowsTransformedImmediately();

	List<Object> getResultList(List<?> results, ResultTransformer resultTransformer) throws QueryException;

}

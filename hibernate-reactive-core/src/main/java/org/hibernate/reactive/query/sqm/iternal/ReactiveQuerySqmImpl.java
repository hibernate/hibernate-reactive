/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.QueryLogging;
import org.hibernate.query.criteria.internal.NamedCriteriaQueryMementoImpl;
import org.hibernate.query.hql.internal.NamedHqlQueryMementoImpl;
import org.hibernate.query.hql.internal.QuerySplitter;
import org.hibernate.query.internal.DelegatingDomainQueryExecutionContext;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.MutableQueryOptions;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.SelectQueryPlan;
import org.hibernate.query.sqm.internal.QuerySqmImpl;
import org.hibernate.query.sqm.internal.SqmInterpretationsKey;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.query.sqm.spi.SelectReactiveQueryPlan;
import org.hibernate.reactive.session.ReactiveQuery;

import jakarta.persistence.NoResultException;

import static org.hibernate.query.spi.SqlOmittingQueryOptions.omitSqlQueryOptions;
import static org.hibernate.query.spi.SqlOmittingQueryOptions.omitSqlQueryOptionsWithUniqueSemanticFilter;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;


public class ReactiveQuerySqmImpl<R> extends QuerySqmImpl<R> implements ReactiveQuery<R> {

	public ReactiveQuerySqmImpl(
			NamedHqlQueryMementoImpl memento,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( memento, expectedResultType, session );
	}

	public ReactiveQuerySqmImpl(
			NamedCriteriaQueryMementoImpl memento,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( memento, resultType, session );
	}

	public ReactiveQuerySqmImpl(
			String hql,
			HqlInterpretation hqlInterpretation,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( hql, hqlInterpretation, resultType, session );
	}

	public ReactiveQuerySqmImpl(
			SqmStatement<R> criteria,
			Class<R> resultType,
			SharedSessionContractImplementor producer) {
		super( criteria, resultType, producer );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return getReactiveResultList()
				.thenCompose( this::reactiveUniqueResultOrFail );
	}

	private CompletionStage<R> reactiveUniqueResultOrFail(List<R> list) {
		return list.isEmpty()
				? failedFuture( new NoResultException( String.format( "No result found for query [%s]", getQueryString() ) ) )
				: completedFuture( uniqueElement( list ) );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return getReactiveResultList()
				.thenCompose( this::reactiveUniqueResultOrNull );
	}

	private CompletionStage<R> reactiveUniqueResultOrNull(List<R> list) {
		try {
			return completedFuture( uniqueElement( list ) );
		}
		catch (HibernateException e) {
			return failedFuture( getSession().getExceptionConverter().convert( e, getLockOptions() ) );
		}
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		beforeQuery();
		return doReactiveList()
				.handle( (list, error) -> {
					handleException( error );
					return list;
				} )
				.whenComplete( (rs, throwable) -> afterQuery( throwable == null ) );
	}

	private void handleException(Throwable e) {
		if ( e != null ) {
			if ( e instanceof IllegalQueryOperationException ) {
				throw new IllegalStateException( e );
			}
			if ( e instanceof TypeMismatchException ) {
				throw new IllegalStateException( e );
			}
			if ( e instanceof HibernateException ) {
				throw getSession().getExceptionConverter()
						.convert( (HibernateException) e, getQueryOptions().getLockOptions() );
			}
			if ( e instanceof RuntimeException ) {
				throw (RuntimeException) e;
			}

			throw new HibernateException( e );
		}
	}

	private CompletionStage<List<R>> doReactiveList() {
		verifySelect();
		getSession().prepareForQueryExecution( requiresTxn( getQueryOptions().getLockOptions().findGreatestLockMode() ) );

		final SqmSelectStatement<?> sqmStatement = (SqmSelectStatement<?>) getSqmStatement();
		final boolean containsCollectionFetches = sqmStatement.containsCollectionFetches();
		final boolean hasLimit = hasLimit( sqmStatement, getQueryOptions() );
		final boolean needsDistinct = containsCollectionFetches
				&& ( sqmStatement.usesDistinct() || hasAppliedGraph( getQueryOptions() ) || hasLimit );

		final DomainQueryExecutionContext executionContextToUse;
		if ( hasLimit && containsCollectionFetches ) {
			boolean fail = getSessionFactory().getSessionFactoryOptions()
					.isFailOnPaginationOverCollectionFetchEnabled();
			if ( fail ) {
				throw new HibernateException(
						"firstResult/maxResults specified with collection fetch. " +
								"In memory pagination was about to be applied. " +
								"Failing because 'Fail on pagination over collection fetch' is enabled."
				);
			}
			else {
				QueryLogging.QUERY_MESSAGE_LOGGER.firstOrMaxResultsSpecifiedWithCollectionFetch();
			}

			final MutableQueryOptions originalQueryOptions = getQueryOptions();
			final QueryOptions normalizedQueryOptions;
			if ( needsDistinct ) {
				normalizedQueryOptions = omitSqlQueryOptionsWithUniqueSemanticFilter( originalQueryOptions, true, false );
			}
			else {
				normalizedQueryOptions = omitSqlQueryOptions( originalQueryOptions, true, false );
			}
			if ( originalQueryOptions == normalizedQueryOptions ) {
				executionContextToUse = this;
			}
			else {
				executionContextToUse = new DelegatingDomainQueryExecutionContext( this ) {
					@Override
					public QueryOptions getQueryOptions() {
						return normalizedQueryOptions;
					}
				};
			}
		}
		else {
			if ( needsDistinct ) {
				final MutableQueryOptions originalQueryOptions = getQueryOptions();
				final QueryOptions normalizedQueryOptions = uniqueSemanticQueryOptions( originalQueryOptions );
				if ( originalQueryOptions == normalizedQueryOptions ) {
					executionContextToUse = this;
				}
				else {
					executionContextToUse = new DelegatingDomainQueryExecutionContext( this ) {
						@Override
						public QueryOptions getQueryOptions() {
							return normalizedQueryOptions;
						}
					};
				}
			}
			else {
				executionContextToUse = this;
			}
		}

		return resolveSelectReactiveQueryPlan()
				.performReactiveList( executionContextToUse )
				.thenApply( (List<R> list) -> needsDistinct
						? applyDistinct( sqmStatement, hasLimit, list )
						: list
				);
	}

	private List<R> applyDistinct(SqmSelectStatement<?> sqmStatement, boolean hasLimit, List<R> list) {
		final int first = !hasLimit || getQueryOptions().getLimit().getFirstRow() == null
				? getIntegerLiteral( sqmStatement.getOffset(), 0 )
				: getQueryOptions().getLimit().getFirstRow();
		final int max = !hasLimit || getQueryOptions().getLimit().getMaxRows() == null
				? getMaxRows( sqmStatement, list.size() )
				: getQueryOptions().getLimit().getMaxRows();
		if ( first > 0 || max != -1 ) {
			final int resultSize = list.size();
			final int toIndex = max != -1
					? first + max
					: resultSize;
			return list.subList( first, toIndex > resultSize ? resultSize : toIndex );
		}
		return list;
	}

	private SelectReactiveQueryPlan<R> resolveSelectReactiveQueryPlan() {
		final QueryInterpretationCache.Key cacheKey = SqmInterpretationsKey.createInterpretationsKey( this );
		if ( cacheKey != null ) {
			return (SelectReactiveQueryPlan<R>) getSession().getFactory()
					.getQueryEngine()
					.getInterpretationCache()
					.resolveSelectQueryPlan( cacheKey, this::buildSelectQueryPlan );
		}
		else {
			return buildSelectQueryPlan();
		}
	}

	private SelectReactiveQueryPlan<R> buildSelectQueryPlan() {
		final SqmSelectStatement<R>[] concreteSqmStatements = QuerySplitter
				.split( (SqmSelectStatement<R>) getSqmStatement(), getSession().getFactory() );

		if ( concreteSqmStatements.length > 1 ) {
			return buildAggregatedSelectQueryPlan( concreteSqmStatements );
		}
		else {
			return buildConcreteSelectQueryPlan( concreteSqmStatements[0], getResultType(), getQueryOptions() );
		}
	}

	private SelectReactiveQueryPlan<R> buildAggregatedSelectQueryPlan(SqmSelectStatement<?>[] concreteSqmStatements) {
		//noinspection unchecked
		final SelectQueryPlan<R>[] aggregatedQueryPlans = new SelectQueryPlan[ concreteSqmStatements.length ];

		// todo (6.0) : we want to make sure that certain thing (ResultListTransformer, etc) only get applied at the aggregator-level

		for ( int i = 0, x = concreteSqmStatements.length; i < x; i++ ) {
			aggregatedQueryPlans[i] = buildConcreteSelectQueryPlan(
					concreteSqmStatements[i],
					getResultType(),
					getQueryOptions()
			);
		}

		throw new NotYetImplementedFor6Exception();
		//		return new AggregatedSelectQueryPlanImpl<>( aggregatedQueryPlans );
	}

	private <T> SelectReactiveQueryPlan<T> buildConcreteSelectQueryPlan(
			SqmSelectStatement<?> concreteSqmStatement,
			Class<T> resultType,
			QueryOptions queryOptions) {
		return new ConcreteSqmSelectReactiveQueryPlan<>(
				concreteSqmStatement,
				getQueryString(),
				getDomainParameterXref(),
				resultType,
				getTupleMetadata(),
				queryOptions
		);
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		throw new NotYetImplementedFor6Exception();
	}
}

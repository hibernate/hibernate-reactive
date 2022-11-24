/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.query.BindableType;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.QueryLogging;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.criteria.internal.NamedCriteriaQueryMementoImpl;
import org.hibernate.query.hql.internal.NamedHqlQueryMementoImpl;
import org.hibernate.query.hql.internal.QuerySplitter;
import org.hibernate.query.internal.DelegatingDomainQueryExecutionContext;
import org.hibernate.query.spi.AbstractSelectionQuery;
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
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.spi.SelectReactiveQueryPlan;
import org.hibernate.reactive.session.ReactiveSqmQueryImplementor;
import org.hibernate.transform.ResultTransformer;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

import static org.hibernate.query.spi.SqlOmittingQueryOptions.omitSqlQueryOptions;
import static org.hibernate.query.spi.SqlOmittingQueryOptions.omitSqlQueryOptionsWithUniqueSemanticFilter;


public class ReactiveQuerySqmImpl<R> extends QuerySqmImpl<R> implements ReactiveSqmQueryImplementor<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

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
	public CompletionStage<R> reactiveUnique() {
		return reactiveList()
				.thenApply( AbstractSelectionQuery::uniqueElement );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return reactiveList()
				.thenApply( this::reactiveSingleResult )
				.exceptionally( this::convertException );
	}

	private R reactiveSingleResult(List<R> list) {
		if ( list.isEmpty() ) {
			throw new NoResultException( String.format( "No result found for query [%s]", getQueryString() ) );
		}
		return uniqueElement( list );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return reactiveList()
				.thenApply( AbstractSelectionQuery::uniqueElement )
				.exceptionally( this::convertException );
	}

	private R convertException(Throwable t) {
		if ( t instanceof HibernateException ) {
			throw getSession().getExceptionConverter().convert( (HibernateException) t, getLockOptions() );
		}
		throw getSession().getExceptionConverter().convert( (RuntimeException) t, getLockOptions() );
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		return reactiveUnique()
				.thenApply( Optional::ofNullable );
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
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
	public List<R> getResultList() {
		return super.getResultList();
	}

	@Override
	public Stream<R> getResultStream() {
		return super.getResultStream();
	}

	@Override
	public int executeUpdate() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		throw new NotYetImplementedFor6Exception();
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// QueryOptions

	@Override
	public ReactiveQuerySqmImpl<R> setHint(String hintName, Object value) {
		super.setHint( hintName, value );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> addQueryHint(String hint) {
		super.addQueryHint( hint );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setLockOptions(LockOptions lockOptions) {
		super.setLockOptions( lockOptions );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setLockMode(String alias, LockMode lockMode) {
		super.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public <T> ReactiveQuerySqmImpl<T> setTupleTransformer(TupleTransformer<T> transformer) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public ReactiveQuerySqmImpl<R> setResultListTransformer(ResultListTransformer transformer) {
		super.setResultListTransformer( transformer );
		return this;
	}

	@Override
	public <T> ReactiveQuerySqmImpl<T> setResultTransformer(ResultTransformer<T> transformer) {
		throw new NotYetImplementedFor6Exception();
	}

	@Override
	public ReactiveQuerySqmImpl<R> setMaxResults(int maxResult) {
		super.setMaxResults( maxResult );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFirstResult(int startPosition) {
		applyFirstResult( startPosition );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		super.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFlushMode(FlushModeType flushMode) {
		super.setFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setLockMode(LockModeType lockMode) {
		super.setLockMode( lockMode );
		return this;
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariance


	@Override
	public ReactiveQuerySqmImpl<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		super.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> applyGraph(RootGraph graph, GraphSemantic semantic) {
		super.applyGraph( graph, semantic );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> applyLoadGraph(RootGraph graph) {
		super.applyLoadGraph( graph );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> applyFetchGraph(RootGraph graph) {
		super.applyFetchGraph( graph );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setComment(String comment) {
		super.setComment( comment );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		super.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		super.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable( cacheable );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheRegion(String cacheRegion) {
		super.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setHibernateLockMode(LockMode lockMode) {
		super.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setTimeout(int timeout) {
		super.setTimeout( timeout );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFetchSize(int fetchSize) {
		super.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setReadOnly(boolean readOnly) {
		super.setReadOnly( readOnly );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setProperties(Object bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setProperties(Map bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Object value) {
		super.setParameter( name, value );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(String name, P value, Class<P> javaType) {
		super.setParameter( name, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(String name, P value, BindableType<P> type) {
		super.setParameter( name, value, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Instant value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Object value) {
		super.setParameter( position, value );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(int position, P value, Class<P> javaType) {
		super.setParameter( position, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(int position, P value, BindableType<P> type) {
		super.setParameter( position, value, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Instant value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(QueryParameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(QueryParameter<P> parameter, P value, Class<P> javaType) {
		super.setParameter( parameter, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(QueryParameter<P> parameter, P value, BindableType<P> type) {
		super.setParameter( parameter, value, type );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(Parameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Calendar value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Date value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Calendar value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Date value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(String name, Collection values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(String name, Object[] values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, P[] values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, P[] values, BindableType<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(int position, Object[] values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, P[] values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, P[] values, BindableType<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, P[] values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFollowOnLocking(boolean enable) {
		super.setFollowOnLocking( enable );
		return this;
	}
}

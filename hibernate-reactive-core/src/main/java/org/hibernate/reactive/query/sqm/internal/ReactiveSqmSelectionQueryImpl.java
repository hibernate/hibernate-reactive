/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;


import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.QueryFlushMode;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.query.QueryLogging;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.internal.DelegatingDomainQueryExecutionContext;
import org.hibernate.query.internal.SelectionQueryImpl;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.MutableQueryOptions;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.internal.ConcreteSqmSelectQueryPlan;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.engine.impl.ReactiveCallbackImpl;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.ReactiveQueryImplementor;
import org.hibernate.reactive.query.sqm.ReactiveSqmSelectionQuery;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;
import org.hibernate.reactive.sql.results.spi.ReactiveSingleResultConsumer;
import org.hibernate.sql.results.internal.TupleMetadata;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static org.hibernate.query.spi.SqlOmittingQueryOptions.omitSqlQueryOptions;

/**
 * A reactive {@link SelectionQueryImpl}
 *
 * @param <R> the result type
 */
public class ReactiveSqmSelectionQueryImpl<R> extends SelectionQueryImpl<R> implements ReactiveSqmSelectionQuery<R>, ReactiveQueryImplementor<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private ReactiveCallbackImpl reactiveCallback;

	public ReactiveSqmSelectionQueryImpl(
			String hql,
			HqlInterpretation hqlInterpretation,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( hql, hqlInterpretation, expectedResultType, null, session );
	}

	public ReactiveSqmSelectionQueryImpl(
			SqmSelectStatement<R> criteria,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( criteria, expectedResultType, session );
	}

	@Override
	public Callback getCallback() {
		if ( reactiveCallback == null ) {
			reactiveCallback = new ReactiveCallbackImpl();
		}
		return reactiveCallback;
	}

	@Override
	public boolean hasCallbackActions() {
		return reactiveCallback != null && reactiveCallback.hasAfterLoadActions();
	}

	@Override
	protected void resetCallback() {
		reactiveCallback = null;
	}


	@Override
	protected <T> ConcreteSqmSelectQueryPlan<T> buildConcreteQueryPlan(
			SqmSelectStatement<T> concreteSqmStatement,
			Class<T> resultType,
			TupleMetadata tupleMetadata,
			QueryOptions queryOptions) {
		return new ConcreteSqmSelectReactiveQueryPlan<>(
				concreteSqmStatement,
				getQueryString(),
				getDomainParameterXref(),
				resultType,
				tupleMetadata,
				queryOptions
		);
	}

	@SuppressWarnings("unchecked")
	private ReactiveSelectQueryPlan<R> resolveSelectReactiveQueryPlan() {
		return (ReactiveSelectQueryPlan<R>) buildConcreteQueryPlan( getSqmStatement() );
	}

	private HashSet<String> reactiveBeforeQuery() {
		return beforeQueryHandlingFetchProfiles();
	}

	private CompletionStage<List<R>> doReactiveList() {
		getSession().prepareForQueryExecution( requiresTxn( getQueryOptions().getLockOptions().findGreatestLockMode() ) );

		final SqmSelectStatement<?> sqmStatement = getSqmStatement();
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
			final QueryOptions normalizedQueryOptions = omitSqlQueryOptions( originalQueryOptions, true, false );
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

		return resolveSelectReactiveQueryPlan()
				.reactivePerformList( executionContextToUse )
				.thenApply( (List<R> list) -> needsDistinct
						? applyDistinct( sqmStatement, hasLimit, list )
						: list
				);
	}

	// Same as in SqmSelectionQueryImpl/ORM's AbstractQuery
	private List<R> applyDistinct(SqmSelectStatement<?> sqmStatement, boolean hasLimit, List<R> list) {
		int includedCount = -1;
		// NOTE : firstRow is zero-based
		final int first = !hasLimit || getQueryOptions().getLimit().getFirstRow() == null
				? getIntegerLiteral( sqmStatement.getOffset(), 0 )
				: getQueryOptions().getLimit().getFirstRow();
		final int max = !hasLimit || getQueryOptions().getLimit().getMaxRows() == null
				? getMaxRows( sqmStatement, list.size() )
				: getQueryOptions().getLimit().getMaxRows();
		final List<R> tmp = new ArrayList<>( list.size() );
		final IdentitySet<Object> distinction = new IdentitySet<>( list.size() );
		for ( final R result : list ) {
			if ( !distinction.add( result ) ) {
				continue;
			}
			includedCount++;
			if ( includedCount < first ) {
				continue;
			}
			tmp.add( result );
			// NOTE : ( max - 1 ) because first is zero-based while max is not...
			if ( max >= 0 && ( includedCount - first ) >= ( max - 1 ) ) {
				break;
			}
		}
		return tmp;
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
		final HashSet<String> fetchProfiles = reactiveBeforeQuery();
		final boolean[] success = {false};
		return doReactiveList()
				.thenApply( list -> {
					success[0] = true;
					return list;
				} )
				.whenComplete( (rs, throwable) -> afterQueryHandlingFetchProfiles( success[0], fetchProfiles ) );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return reactiveList().thenApply( this::reactiveSingleResult );
	}

	private R reactiveSingleResult(List<R> list) {
		if ( list.isEmpty() ) {
			throw new jakarta.persistence.NoResultException(
					String.format( "No result found for query [%s]", getQueryString() )
			);
		}
		return reactiveUniqueElement( list );
	}

	private static <T> T reactiveUniqueElement(List<T> list) {
		try {
			return uniqueElement( list );
		}
		catch (org.hibernate.NonUniqueResultException e) {
			throw new jakarta.persistence.NonUniqueResultException( e.getMessage() );
		}
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return reactiveList().thenApply( ReactiveSqmSelectionQueryImpl::reactiveUniqueElement );
	}

	@Override
	public CompletionStage<R> reactiveUnique() {
		return reactiveList().thenApply( ReactiveSqmSelectionQueryImpl::reactiveUniqueElement );
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		return reactiveUnique().thenApply( Optional::ofNullable );
	}

	@Override
	public CompletionStage<Long> getReactiveResultCount() {
		final DelegatingDomainQueryExecutionContext context = new DelegatingDomainQueryExecutionContext( this ) {
			@Override
			public QueryOptions getQueryOptions() {
				return QueryOptions.NONE;
			}
		};
		return ( (ReactiveSelectQueryPlan<Long>) buildConcreteQueryPlan( getSqmStatement().createCountQuery(), Long.class, null, getQueryOptions() ) )
				.reactiveExecuteQuery( context, new ReactiveSingleResultConsumer<>() );
	}

	private CompletionStage<Long> reactiveExecuteCount(DomainQueryExecutionContext context) {
		return ( (ReactiveSelectQueryPlan<Long>) buildConcreteQueryPlan(
				getSqmStatement().createCountQuery(),
				Long.class,
				null,
				getQueryOptions()
		) ).reactiveExecuteQuery( context, new ReactiveSingleResultConsumer<>() );
	}

	@Override
	public R getSingleResult() {
		throw LOG.nonReactiveMethodCall( "getReactiveSingleResult" );
	}

	@Override
	public R getSingleResultOrNull() {
		throw LOG.nonReactiveMethodCall( "getReactiveSingleResultOrNull" );
	}

	@Override
	public List<R> getResultList() {
		throw LOG.nonReactiveMethodCall( "getReactiveResultList" );
	}

	@Override
	public Stream<R> getResultStream() {
		throw LOG.nonReactiveMethodCall( "<no alternative>" );
	}

	@Override
	public LockOptions getLockOptions() {
		return getQueryOptions().getLockOptions();
	}

	@Override
	public void applyGraph(RootGraphImplementor<?> graph, GraphSemantic semantic) {
		getQueryOptions().applyGraph( graph, semantic );
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setFlushMode(FlushModeType flushMode) {
		super.setFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setLockMode(LockModeType lockMode) {
		super.setLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setHibernateLockMode(LockMode lockMode) {
		super.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setLockMode(String alias, LockMode lockMode) {
		getQueryOptions().getLockOptions().setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setFollowOnLocking(boolean enable) {
		super.setFollowOnLockingStrategy( enable
				? org.hibernate.Locking.FollowOn.FORCE
				: org.hibernate.Locking.FollowOn.DISALLOW );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setFetchSize(int fetchSize) {
		super.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setReadOnly(boolean readOnly) {
		super.setReadOnly( readOnly );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		super.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		super.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable( cacheable );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setCacheRegion(String regionName) {
		super.setCacheRegion( regionName );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		setQueryFlushMode( QueryFlushMode.fromHibernateMode( flushMode ) );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setTimeout(int timeout) {
		super.setTimeout( timeout );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setMaxResults(int maxResult) {
		super.setMaxResults( maxResult );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setFirstResult(int startPosition) {
		super.setFirstResult( startPosition );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setHint(String hintName, Object value) {
		super.setHint( hintName, value );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(String name, Object value) {
		super.setParameter( name, value );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(String name, P value, Class<P> javaType) {
		super.setParameter( name, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(String name, P value, Type<P> type) {
		super.setParameter( name, value, type );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(String name, Instant value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(int position, Object value) {
		super.setParameter( position, value );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(int position, P value, Class<P> javaType) {
		super.setParameter( position, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(int position, P value, Type<P> type) {
		super.setParameter( position, value, type );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(int position, Instant value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(QueryParameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(QueryParameter<P> parameter, P value, Class<P> javaType) {
		super.setParameter( parameter, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(
			QueryParameter<P> parameter,
			P value,
			Type<P> type) {
		super.setParameter( parameter, value, type );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(Parameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(
			Parameter<Calendar> param,
			Calendar value,
			TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(String name, Calendar value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(String name, Date value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(int position, Calendar value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameter(int position, Date value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameterList(String name, Collection values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			String name,
			Collection<? extends P> values,
			Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			String name,
			Collection<? extends P> values,
			Type<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameterList(String name, Object[] values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(String name, P[] values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(String name, P[] values, Type<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameterList(int position, Collection values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			int position,
			Collection<? extends P> values,
			Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			int position,
			Collection<? extends P> values,
			Type<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setParameterList(int position, Object[] values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(int position, P[] values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(int position, P[] values, Type<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Type<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			QueryParameter<P> parameter,
			P[] values,
			Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(
			QueryParameter<P> parameter,
			P[] values,
			Type<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setProperties(Map map) {
		super.setProperties( map );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setProperties(Object bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> enableFetchProfile(String profileName) {
		super.enableFetchProfile( profileName );
		return this;
	}

	// ReactiveQueryImplementor methods

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setComment(String comment) {
		super.setComment( comment );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> addQueryHint(String hint) {
		super.addQueryHint( hint );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setLockOptions(LockOptions lockOptions) {
		// Apply lock options to the query
		if ( lockOptions != null ) {
			getQueryOptions().getLockOptions().overlay( lockOptions );
		}
		return this;
	}

	@Override
	public QueryParameterBindings getParameterBindings() {
		return getQueryParameterBindings();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> ReactiveSqmSelectionQueryImpl<T> setTupleTransformer(TupleTransformer<T> transformer) {
		super.setTupleTransformer( transformer );
		return (ReactiveSqmSelectionQueryImpl<T>) this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setResultListTransformer(ResultListTransformer<R> transformer) {
		super.setResultListTransformer( transformer );
		return this;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		throw new IllegalQueryOperationException( "Not a mutation query" );
	}
}

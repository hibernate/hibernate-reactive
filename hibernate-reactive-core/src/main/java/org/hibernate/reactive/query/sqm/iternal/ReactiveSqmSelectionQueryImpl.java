/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;


import java.time.Instant;
import java.util.ArrayList;
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
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.query.BindableType;
import org.hibernate.query.QueryLogging;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.criteria.internal.NamedCriteriaQueryMementoImpl;
import org.hibernate.query.hql.internal.NamedHqlQueryMementoImpl;
import org.hibernate.query.internal.DelegatingDomainQueryExecutionContext;
import org.hibernate.query.spi.AbstractSelectionQuery;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.MutableQueryOptions;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.internal.SqmSelectionQueryImpl;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.query.ReactiveSelectionQuery;
import org.hibernate.reactive.query.spi.ReactiveAbstractSelectionQuery;
import org.hibernate.reactive.query.sqm.ReactiveSqmSelectionQuery;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

import static org.hibernate.query.spi.SqlOmittingQueryOptions.omitSqlQueryOptions;

/**
 * A reactive {@link SqmSelectionQueryImpl}
 * @param <R>
 */
public class ReactiveSqmSelectionQueryImpl<R> extends SqmSelectionQueryImpl<R> implements ReactiveSqmSelectionQuery<R> {

	private final ReactiveAbstractSelectionQuery<R> selectionQueryDelegate;

	public ReactiveSqmSelectionQueryImpl(
			String hql,
			HqlInterpretation hqlInterpretation,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( hql, hqlInterpretation, expectedResultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveSqmSelectionQueryImpl(
			NamedHqlQueryMementoImpl memento,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( memento, resultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveSqmSelectionQueryImpl(
			NamedCriteriaQueryMementoImpl memento,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( memento, resultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveSqmSelectionQueryImpl(
			SqmSelectStatement criteria,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( criteria, expectedResultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	private ReactiveAbstractSelectionQuery<R> createSelectionQueryDelegate(SharedSessionContractImplementor session) {
		return new ReactiveAbstractSelectionQuery<>(
				this,
				session,
				this::doReactiveList,
				this::getSqmStatement,
				this::getTupleMetadata,
				this::getDomainParameterXref,
				this::getResultType,
				this::getQueryString,
				this::beforeQuery,
				this::afterQuery,
				AbstractSelectionQuery::uniqueElement
		);
	}

	private CompletionStage<List<R>> doReactiveList() {
		getSession().prepareForQueryExecution( requiresTxn( getQueryOptions().getLockOptions()
																	.findGreatestLockMode() ) );

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

		return selectionQueryDelegate.resolveSelectReactiveQueryPlan()
				.reactivePerformList( executionContextToUse )
				.thenApply( (List<R> list) -> needsDistinct
						? applyDistinct( sqmStatement, hasLimit, list )
						: list
				);
	}

	// I would expect this to be the same as the one in ReactiveSqmQueryImpl.
	// But in ORM the code is not exactly the same, see SqmSelectionQueryImpl and SqmQueryImpl
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
	public ReactiveSqmSelectionQueryImpl<R> setFlushMode(FlushModeType flushMode) {
		super.setFlushMode( flushMode );
		return this;
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return selectionQueryDelegate.getReactiveSingleResult();
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
		return selectionQueryDelegate.reactiveList();
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return selectionQueryDelegate.getReactiveSingleResultOrNull();
	}

	@Override
	public CompletionStage<R> reactiveUnique() {
		return selectionQueryDelegate.reactiveUnique();
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		return selectionQueryDelegate.reactiveUniqueResultOptional();
	}

	@Override
	public R getSingleResult() {
		return selectionQueryDelegate.getSingleResult();
	}

	@Override
	public R getSingleResultOrNull() {
		return selectionQueryDelegate.getSingleResultOrNull();
	}

	@Override
	public List<R> getResultList() {
		return selectionQueryDelegate.getResultList();
	}

	@Override
	public Stream<R> getResultStream() {
		return selectionQueryDelegate.getResultStream();
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
	public ReactiveSqmSelectionQueryImpl<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		super.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setLockMode(String alias, LockMode lockMode) {
		super.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public ReactiveSqmSelectionQueryImpl<R> setFollowOnLocking(boolean enable) {
		super.setFollowOnLocking( enable );
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
		super.setHibernateFlushMode( flushMode );
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
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(String name, P value, BindableType<P> type) {
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
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameter(int position, P value, BindableType<P> type) {
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
			BindableType<P> type) {
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
			BindableType<P> type) {
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
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(String name, P[] values, BindableType<P> type) {
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
			BindableType<P> type) {
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
	public <P> ReactiveSqmSelectionQueryImpl<R> setParameterList(int position, P[] values, BindableType<P> type) {
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
			BindableType<P> type) {
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
			BindableType<P> type) {
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
	public ReactiveSelectionQuery<R> applyGraph(RootGraph<?> graph, GraphSemantic semantic) {
		getQueryOptions().applyGraph( (RootGraphImplementor<?>) graph, semantic );
		return this;
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.query.criteria.internal.compile.ExplicitParameterInfo;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.internal.QueryImpl;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.transform.ResultTransformer;

import javax.persistence.EntityGraph;
import javax.persistence.Parameter;
import javax.persistence.criteria.ParameterExpression;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.emptyMap;
import static org.hibernate.reactive.session.ReactiveQuery.convertQueryException;
import static org.hibernate.reactive.session.ReactiveQuery.extractUniqueResult;
import static org.hibernate.reactive.session.ReactiveQuery.extractUniqueResultOrNull;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * Implementation of {@link ReactiveQuery} by extension of
 * {@link QueryImpl}.
 */
public class ReactiveQueryImpl<R> extends QueryImpl<R> implements ReactiveQuery<R> {

	/**
	 * Needed once we support query hints.
	 *
	 * See {@link #collectHints}
	 */
	private EntityGraphQueryHint entityGraphQueryHint;
	private Map<ParameterExpression<?>, ExplicitParameterInfo<?>> explicitParameterInfoMap;
	private final QueryType type;
	private Object optionalObject;
	private String optionalEntityName;
	private Serializable optionalId;
	private ResultTransformer resultTransformer;

	private final List<String> dbHints = new ArrayList<>();

	private Boolean passDistinctThrough;

	private static QueryType queryType(String queryString) {
		queryString = queryString.trim().toLowerCase();
		return queryString.startsWith("insert")
			|| queryString.startsWith("update")
			|| queryString.startsWith("delete")
				? QueryType.INSERT_UPDATE_DELETE
				: QueryType.SELECT;
	}
	public ReactiveQueryImpl(SharedSessionContractImplementor producer,
							 HQLQueryPlan hqlQueryPlan,
							 String queryString) {
		super( producer, hqlQueryPlan, queryString );
		this.type = queryType( queryString );
	}

	private String expandedQuery() {
		return getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
	}

	@Override
	public void setParameterMetadata(InterpretedParameterMetadata parameterMetadata) {
		explicitParameterInfoMap = parameterMetadata.explicitParameterInfoMap();
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		if ( type!=null && type!=QueryType.SELECT ) {
			throw new UnsupportedOperationException("not a select query");
		}
		return getReactiveResultList().thenApply( list -> extractUniqueResult( list, this ) );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		if ( type!=null && type!=QueryType.SELECT ) {
			throw new UnsupportedOperationException("not a select query");
		}
		return getReactiveResultList().thenApply( list -> extractUniqueResultOrNull( list, this ) );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		if ( type!=null && type!=QueryType.INSERT_UPDATE_DELETE ) {
			throw new UnsupportedOperationException("not an insert/update/delete query");
		}

		getProducer().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );

		beforeQuery();
		String expanded = expandedQuery();
		return reactiveProducer()
				.executeReactiveUpdate( expanded, makeQueryParametersForExecution( expanded ) )
				.whenComplete( (count, error) -> afterQuery() )
				.handle( (count, error) -> convertQueryException( count, error, this ) );
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		if ( type!=null && type!=QueryType.SELECT ) {
			throw new UnsupportedOperationException("not a select query");
		}
		beforeQuery();
		return doReactiveList()
				.whenComplete( (list, err) -> afterQuery() )
				.handle( (count, error) -> convertQueryException( count, error, this ) );
	}

	private CompletionStage<List<R>> doReactiveList() {
		if ( getMaxResults() == 0 ) {
			return completedFuture( Collections.emptyList() );
		}
		else {
			// disable this check for now because I don't have a
			// way figure out if there is a transaction in process,
			// and anyway we don't really care about this check
	//		LockMode lockMode = getLockOptions().getLockMode();
	//		if ( lockMode != null && lockMode != LockMode.NONE ) {
	//			// note that this check doesn't get done for aliased locks,
	//			// but that's the same as in hibernate-core so don't care
	//			if ( !getProducer().isTransactionInProgress() ) {
	//				throw new TransactionRequiredException( "no transaction is in progress" );
	//			}
	//		}

			String expanded = expandedQuery();
			return reactiveProducer()
					.reactiveList( expanded, makeQueryParametersForExecution( expanded ) );
		}
	}

	private ReactiveQueryExecutor reactiveProducer() {
		return (ReactiveQueryExecutor) getProducer();
	}

	@Override
	public void setOptionalObject(Object optionalObject) {
		this.optionalObject = optionalObject;
		super.setOptionalObject( optionalObject );
	}

	@Override
	public void setOptionalEntityName(String optionalEntityName) {
		this.optionalEntityName = optionalEntityName;
		super.setOptionalEntityName( optionalEntityName );
	}

	@Override
	public void setOptionalId(Serializable optionalId) {
		this.optionalId = optionalId;
		super.setOptionalId( optionalId );
	}

	@Override
	protected boolean applyPassDistinctThrough(boolean passDistinctThrough) {
		this.passDistinctThrough = passDistinctThrough;
		return super.applyPassDistinctThrough( passDistinctThrough );
	}

	@Override
	public QueryImplementor addQueryHint(String hint) {
		this.dbHints.add( hint );
		return super.addQueryHint( hint );
	}

	/**
	 * Overrides {@link org.hibernate.query.internal.AbstractProducedQuery#makeQueryParametersForExecution(String)}
	 * so that we can return a {@link ReactiveHQLQueryPlan}.
	 */
	@Override
	protected QueryParameters makeQueryParametersForExecution(String hql) {
		final HQLQueryPlan entityGraphHintedQueryPlan;
		if ( entityGraphQueryHint == null ) {
			entityGraphHintedQueryPlan = null;
		}
		else {
			final SharedSessionContractImplementor producer = getProducer();
			entityGraphHintedQueryPlan = new ReactiveHQLQueryPlan<>(
					hql,
					false,
					producer.getLoadQueryInfluencers().getEnabledFilters(),
					producer.getFactory(),
					entityGraphQueryHint
			);
		}

		QueryParameters queryParameters = new QueryParameters(
				getQueryParameterBindings(),
				getLockOptions(),
				getQueryOptions(),
				isReadOnly(),
				isReadOnly(),
				isCacheable(),
				getCacheRegion(),
				getComment(),
				dbHints,
				null,
				optionalObject,
				optionalEntityName,
				optionalId,
				resultTransformer
		);

		appendQueryPlanToQueryParameters( hql, queryParameters, entityGraphHintedQueryPlan );

		if ( passDistinctThrough != null ) {
			queryParameters.setPassDistinctThrough( passDistinctThrough );
		}
		return queryParameters;
	}

	@Override
	public void applyEntityGraphQueryHint(EntityGraphQueryHint entityGraphQueryHint) {
		super.applyEntityGraphQueryHint(entityGraphQueryHint);
		this.entityGraphQueryHint = entityGraphQueryHint;
	}

	@Override
	public ReactiveQueryImpl<R> setParameter(int position, Object value) {
		super.setParameter(position, value);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setParameter(String name, Object value) {
		super.setParameter(name, value);
		return this;
	}

	@Override
	public <P> ReactiveQueryImpl<R> setParameter(Parameter<P> parameter, P value) {
		if (explicitParameterInfoMap==null) {
			// not a criteria query
			super.setParameter( parameter, value );
		}
		else {
			final ExplicitParameterInfo<?> parameterInfo = resolveParameterInfo( parameter );
			if ( parameterInfo.isNamed() ) {
				setParameter( parameterInfo.getName(), value );
			}
			else {
				setParameter( parameterInfo.getPosition(), value );
			}
		}
		return this;
	}

	private <T> ExplicitParameterInfo<?> resolveParameterInfo(Parameter<T> param) {
		if (param instanceof ExplicitParameterInfo) {
			return (ExplicitParameterInfo<?>) param;
		}
		else if (param instanceof ParameterExpression) {
			return explicitParameterInfoMap.get( param );
		}
		else {
			for ( ExplicitParameterInfo<?> parameterInfo: explicitParameterInfoMap.values() ) {
				if ( param.getName() != null && param.getName().equals( parameterInfo.getName() ) ) {
					return parameterInfo;
				}
				else if ( param.getPosition() != null && param.getPosition().equals( parameterInfo.getPosition() ) ) {
					return parameterInfo;
				}
			}
		}
		throw new IllegalArgumentException( "Unable to locate parameter [" + param + "] in query" );
	}

	@Override
	public ReactiveQueryImpl<R> setMaxResults(int maxResults) {
		super.setMaxResults(maxResults);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setFirstResult(int firstResult) {
		super.setFirstResult(firstResult);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setReadOnly(boolean readOnly) {
		super.setReadOnly(readOnly);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setComment(String comment) {
		super.setComment(comment);
		return this;
	}

	@Override
	public ReactiveQuery<R> setLockMode(LockMode lockMode) {
		getProducer().checkOpen();
		if ( !LockMode.NONE.equals( lockMode ) ) {
			@SuppressWarnings("deprecation")
			boolean select = getProducer().getFactory().getQueryPlanCache()
					.getHQLQueryPlan( getQueryString(), false, emptyMap() )
					.isSelect();
			if ( !select) {
				throw new IllegalArgumentException( "Lock mode is only supported for select queries: " + lockMode );
			}
		}
		getLockOptions().setLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setLockOptions(LockOptions lockOptions) {
		super.setLockOptions(lockOptions);
		return this;
	}

	@Override
	public ReactiveQuery<R> setQueryHint(String hintName, Object value) {
		super.setHint(hintName, value);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setLockMode(String alias, LockMode lockMode) {
		super.setLockMode(alias, lockMode);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode(cacheMode);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		super.setHibernateFlushMode(flushMode);
		return this;
	}

	@Override
	public FlushMode getHibernateFlushMode() {
		return super.getHibernateFlushMode();
	}

	@Override
	public ReactiveQueryImpl<R> setResultTransformer(ResultTransformer resultTransformer) {
		this.resultTransformer = resultTransformer;
		super.setResultTransformer(resultTransformer);
		return this;
	}

	@Override
	public void setPlan(EntityGraph<R> entityGraph) {
		applyGraph( (RootGraph<?>) entityGraph, GraphSemantic.FETCH );
		applyEntityGraphQueryHint( new EntityGraphQueryHint( GraphSemantic.FETCH.getJpaHintName(), entityGraph ) );
	}

	@Override
	public ReactiveQueryImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable(cacheable);
		return this;
	}

	@Override
	public ReactiveQueryImpl<R> setCacheRegion(String cacheRegion) {
		super.setCacheRegion(cacheRegion);
		return this;
	}

	@Override
	public ReactiveQuery<R> setQuerySpaces(String[] querySpaces) {
		throw new UnsupportedOperationException();
	}
}

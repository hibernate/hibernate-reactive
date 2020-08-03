/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.internal.RootGraphImpl;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.criteria.internal.compile.ExplicitParameterInfo;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.internal.QueryImpl;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;

import javax.persistence.EntityGraph;
import javax.persistence.Parameter;
import javax.persistence.criteria.ParameterExpression;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.emptyMap;
import static org.hibernate.jpa.QueryHints.HINT_FETCHGRAPH;
import static org.hibernate.reactive.session.ReactiveQuery.convertQueryException;
import static org.hibernate.reactive.session.ReactiveQuery.extractUniqueResult;

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

	public ReactiveQueryImpl(SharedSessionContractImplementor producer,
							 ParameterMetadata parameterMetadata,
							 String queryString,
							 QueryType type) {
		super( producer, parameterMetadata, queryString );
		this.type = type;
	}

	@Override
	public void setParameterMetadata(InterpretedParameterMetadata parameterMetadata) {
		explicitParameterInfoMap = parameterMetadata.explicitParameterInfoMap();
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		if (type!=null && type!=QueryType.SELECT) {
			throw new UnsupportedOperationException("not a select query");
		}
		return getReactiveResultList().thenApply( list -> extractUniqueResult( list, this ) );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		if (type!=null && type!=QueryType.INSERT_UPDATE_DELETE) {
			throw new UnsupportedOperationException("not an insert/update/delete query");
		}

		getProducer().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );

		beforeQuery();
		return doExecuteReactiveUpdate()
				.whenComplete( (count, error) -> afterQuery() )
				.handle( (count, error) -> convertQueryException( count, error, this ) );
	}

	//copy pasted between here and ReactiveNativeQueryImpl
	private CompletionStage<Integer> doExecuteReactiveUpdate() {
		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		return reactiveProducer().executeReactiveUpdate( expandedQuery, makeQueryParametersForExecution( expandedQuery ) );
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		if (type!=null && type!=QueryType.SELECT) {
			throw new UnsupportedOperationException("not a select query");
		}
		beforeQuery();
		return doReactiveList()
				.whenComplete( (list, err) -> afterQuery() )
				.handle( (count, error) -> convertQueryException( count, error, this ) );
	}

	private CompletionStage<List<R>> doReactiveList() {
		if ( getMaxResults() == 0 ) {
			return CompletionStages.completedFuture( Collections.emptyList() );
		}

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

		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		return reactiveProducer().reactiveList( expandedQuery, makeReactiveQueryParametersForExecution( expandedQuery ) );
	}

	private ReactiveQueryExecutor reactiveProducer() {
		return (ReactiveQueryExecutor) getProducer();
	}

	/**
	 * @see #makeQueryParametersForExecution(String)
	 */
	private QueryParameters makeReactiveQueryParametersForExecution(String hql) {
		QueryParameters queryParameters = super.makeQueryParametersForExecution( hql );
		if ( queryParameters.getQueryPlan() != null ) {
			HQLQueryPlan plan = new ReactiveHQLQueryPlan(
					hql,
					false,
					getProducer().getLoadQueryInfluencers().getEnabledFilters(),
					getProducer().getFactory(),
					entityGraphQueryHint
			);
			queryParameters.setQueryPlan( plan );
		}
		return queryParameters;
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
		super.setResultTransformer(resultTransformer);
		return this;
	}

	@Override
	public void setPlan(EntityGraph<R> entityGraph) {
		applyGraph( (RootGraph) entityGraph, GraphSemantic.FETCH );
		applyEntityGraphQueryHint( new EntityGraphQueryHint( HINT_FETCHGRAPH, (RootGraphImpl) entityGraph ) );
	}
}

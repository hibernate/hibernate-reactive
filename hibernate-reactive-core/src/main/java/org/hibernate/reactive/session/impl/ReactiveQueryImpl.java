/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.LockMode;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.internal.QueryImpl;
import org.hibernate.reactive.session.QueryType;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;

import javax.persistence.TransactionRequiredException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

	private final QueryType type;

	public ReactiveQueryImpl(SharedSessionContractImplementor producer,
							 ParameterMetadata parameterMetadata,
							 String queryString,
							 QueryType type) {
		super( producer, parameterMetadata, queryString );
		this.type = type;
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
		if ( getLockOptions().getLockMode() != null && getLockOptions().getLockMode() != LockMode.NONE ) {
			if ( !getProducer().isTransactionInProgress() ) {
				throw new TransactionRequiredException( "no transaction is in progress" );
			}
		}

		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		return reactiveProducer().reactiveList( expandedQuery, makeReactiveQueryParametersForExecution( expandedQuery ) );
	}

	private ReactiveSession reactiveProducer() {
		return (ReactiveSession) getProducer();
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
	public ReactiveQueryImpl<R> setResultTransformer(ResultTransformer resultTransformer) {
		super.setResultTransformer(resultTransformer);
		return this;
	}


}

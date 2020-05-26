package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.internal.QueryImpl;
import org.hibernate.reactive.session.QueryType;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;

import javax.persistence.NoResultException;
import javax.persistence.TransactionRequiredException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

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
		return getReactiveResultList().thenApply( this::uniqueResult );
	}

	private R uniqueResult(List<R> list) {
		try {
			if ( list.size() == 0 ) {
				throw new NoResultException( "No entity found for query" );
			}
			return uniqueElement( list );
		}
		catch (HibernateException e) {
			throw getExceptionConverter().convert( e, getLockOptions() );
		}
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		if (type!=null && type!=QueryType.INSERT_UPDATE_DELETE) {
			throw new UnsupportedOperationException("not an insert/update/delete query");
		}

		getProducer().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );

		beforeQuery();
		return doExecuteReactiveUpdate()
				.handle( (count, e) -> {
					afterQuery();
					if ( e instanceof QueryExecutionRequestException ) {
						CompletionStages.rethrow( new IllegalStateException( e ) );
					}
					if ( e instanceof TypeMismatchException ) {
						CompletionStages.rethrow( new IllegalStateException( e ) );
					}
					if ( e instanceof HibernateException ) {
						CompletionStages.rethrow( getExceptionConverter().convert( (HibernateException) e ) );
					}
					CompletionStages.rethrowIfNotNull( e );
					return count;
				} );
	}

	private CompletionStage<Integer> doExecuteReactiveUpdate() {
		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		return reactiveProducer().executeReactiveUpdate( expandedQuery, makeQueryParametersForExecution( expandedQuery ) );
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		if (type!=null && type!=QueryType.SELECT) {
			throw new UnsupportedOperationException("not a select query");
		}
		return reactiveList();
	}

	private CompletionStage<List<R>> reactiveList() {
		beforeQuery();
		return doReactiveList().whenComplete( (list, err) -> afterQuery() );
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
		ReactiveSession producer = reactiveProducer();
		return producer.reactiveList( expandedQuery, makeReactiveQueryParametersForExecution( expandedQuery ) );
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

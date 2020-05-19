package org.hibernate.reactive.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import javax.persistence.NoResultException;
import javax.persistence.TransactionRequiredException;

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
import org.hibernate.reactive.engine.query.spi.ReactiveHQLQueryPlan;
import org.hibernate.reactive.util.impl.CompletionStages;

public class ReactiveQueryInternalImpl<R> extends QueryImpl<R> implements ReactiveQueryInternal<R> {

	private EntityGraphQueryHint entityGraphQueryHint;

	public ReactiveQueryInternalImpl(SharedSessionContractImplementor producer, ParameterMetadata parameterMetadata, String queryString) {
		super( producer, parameterMetadata, queryString );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
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

	protected CompletionStage<Integer> doExecuteReactiveUpdate() {
		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		return reactiveProducer().executeReactiveUpdate( expandedQuery, makeQueryParametersForExecution( expandedQuery ) );
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		return reactiveList();
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
		beforeQuery();
		return doReactiveList().handle( (list, err) -> {
			afterQuery();
			CompletionStages.rethrowIfNotNull( err );
			//TODO: this typecast is rubbish!
			return (List<R>) list;
		});
	}

	@SuppressWarnings("unchecked")
	protected CompletionStage<List<Object>> doReactiveList() {
		if ( getMaxResults() == 0 ) {
			return CompletionStages.completedFuture( Collections.EMPTY_LIST );
		}
		if ( getLockOptions().getLockMode() != null && getLockOptions().getLockMode() != LockMode.NONE ) {
			if ( !getProducer().isTransactionInProgress() ) {
				throw new TransactionRequiredException( "no transaction is in progress" );
			}
		}

		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		ReactiveSessionInternal producer = reactiveProducer();
		return producer.reactiveList( expandedQuery, makeReactiveQueryParametersForExecution( expandedQuery ) );
	}

	private ReactiveSessionInternal reactiveProducer() {
		return (ReactiveSessionInternal) getProducer();
	}

	/**
	 * @see #makeQueryParametersForExecution(String)
	 */
	protected QueryParameters makeReactiveQueryParametersForExecution(String hql) {
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

	@Deprecated
	protected void applyEntityGraphQueryHint(EntityGraphQueryHint hint) {
		super.applyEntityGraphQueryHint( hint );
		this.entityGraphQueryHint = hint;
	}
}

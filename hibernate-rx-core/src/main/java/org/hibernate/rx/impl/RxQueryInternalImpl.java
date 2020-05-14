package org.hibernate.rx.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import javax.persistence.NoResultException;
import javax.persistence.TransactionRequiredException;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.internal.QueryImpl;
import org.hibernate.rx.RxQueryInternal;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.engine.query.spi.RxHQLQueryPlan;
import org.hibernate.rx.util.impl.RxUtil;

public class RxQueryInternalImpl<R> extends QueryImpl<R> implements RxQueryInternal<R> {

	private EntityGraphQueryHint entityGraphQueryHint;

	public RxQueryInternalImpl(SharedSessionContractImplementor producer, ParameterMetadata parameterMetadata, String queryString) {
		super( producer, parameterMetadata, queryString );
	}

	@Override
	public CompletionStage<R> getRxSingleResult() {
		return getRxResultList().thenApply( l -> uniqueResult( l ) );
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
	public CompletionStage<List<R>> getRxResultList() {
		return rxList();
	}

	@Override
	public CompletionStage<List<R>> rxList() {
		beforeQuery();
		return doRxList().handle( (list, err) -> {
			afterQuery();
			if ( err != null ) {
				RxUtil.rethrow( err );
			}
			return list;
		});
	}

	@SuppressWarnings("unchecked")
	protected CompletionStage<List<R>> doRxList() {
		if ( getMaxResults() == 0 ) {
			return RxUtil.completedFuture( Collections.EMPTY_LIST );
		}
		if ( getLockOptions().getLockMode() != null && getLockOptions().getLockMode() != LockMode.NONE ) {
			if ( !getProducer().isTransactionInProgress() ) {
				throw new TransactionRequiredException( "no transaction is in progress" );
			}
		}

		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		RxSessionInternal producer = (RxSessionInternal) getProducer();
		return producer.rxList( expandedQuery, makeRxQueryParametersForExecution( expandedQuery ) );
	}

	/**
	 * @see #makeQueryParametersForExecution(String)
	 */
	protected QueryParameters makeRxQueryParametersForExecution(String hql) {
		QueryParameters queryParameters = super.makeQueryParametersForExecution( hql );
		if ( queryParameters.getQueryPlan() != null ) {
			HQLQueryPlan plan = new RxHQLQueryPlan(
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

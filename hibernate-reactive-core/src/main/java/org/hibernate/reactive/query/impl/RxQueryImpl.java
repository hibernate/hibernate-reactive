package org.hibernate.reactive.query.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.CacheMode;
import org.hibernate.reactive.stage.RxQuery;
import org.hibernate.reactive.internal.RxQueryInternal;

public class RxQueryImpl<R> implements RxQuery<R> {

	private final RxQueryInternal<R> delegate;

	public RxQueryImpl(RxQueryInternal<R> delegate) {
		this.delegate = delegate;
	}

	@Override
	public RxQuery setParameter(int var1, Object var2) {
		delegate.setParameter( var1, var2 );
		return this;
	}

	@Override
	public RxQuery setMaxResults(int maxResults) {
		delegate.setMaxResults( maxResults );
		return this;
	}

	@Override
	public RxQuery setFirstResult(int firstResult) {
		delegate.setFirstResult( firstResult );
		return this;
	}

	@Override
	public RxQuery<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public RxQuery<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public RxQuery<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	@Override
	public CompletionStage<Integer> executeUpdate() {
		return delegate.executeRxUpdate();
	}

	@Override
	public CompletionStage<R> getSingleResult() {
		return delegate.getRxSingleResult();
	}

	@Override
	public CompletionStage<List<R>> getResultList() {
		return delegate.getRxResultList();
	}

}

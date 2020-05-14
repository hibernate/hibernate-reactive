package org.hibernate.rx.query.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.query.Query;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxQueryInternal;

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
	public RxQuery setMaxResults(int var1) {
		delegate.setMaxResults( var1 );
		return this;
	}

	@Override
	public RxQuery setFirstResult(int var1) {
		delegate.setFirstResult( var1 );
		return this;
	}

	@Override
	public CompletionStage<R> getSingleResult() {
		return delegate.getRxSingleResult();
	}

	@Override
	public CompletionStage<List<R>> getResultList() {
		return delegate.getRxResultList();
	}

	@Override
	public <T> T unwrap(Class<T> type) {
		if ( RxQueryImpl.class.isAssignableFrom( type ) ) {
			return (T) this;
		}
		if ( Query.class.isAssignableFrom( type ) ) {
			return (T) delegate;
		}
		throw new AssertionFailure( type + " not recognized" );
	}
}

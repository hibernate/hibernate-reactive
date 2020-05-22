package org.hibernate.reactive.stage.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.CacheMode;
import org.hibernate.LockMode;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.stage.Stage;

/**
 * Implementation of {@link Stage.Query}.
 */
public class StageQueryImpl<R> implements Stage.Query<R> {

	private final ReactiveQuery<R> delegate;

	public StageQueryImpl(ReactiveQuery<R> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Stage.Query<R> setParameter(int var1, Object var2) {
		delegate.setParameter( var1, var2 );
		return this;
	}

	@Override
	public Stage.Query<R> setMaxResults(int maxResults) {
		delegate.setMaxResults( maxResults );
		return this;
	}

	@Override
	public Stage.Query<R> setFirstResult(int firstResult) {
		delegate.setFirstResult( firstResult );
		return this;
	}

	@Override
	public Stage.Query<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public Stage.Query<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public Stage.Query<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Stage.Query<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	@Override
	public CompletionStage<Integer> executeUpdate() {
		return delegate.executeReactiveUpdate();
	}

	@Override
	public CompletionStage<R> getSingleResult() {
		return delegate.getReactiveSingleResult();
	}

	@Override
	public CompletionStage<List<R>> getResultList() {
		return delegate.getReactiveResultList();
	}

}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.stage.Stage;

import javax.persistence.Parameter;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Implementation of {@link Stage.Query}.
 */
public class StageQueryImpl<R> implements Stage.Query<R> {

	private final ReactiveQuery<R> delegate;

	public StageQueryImpl(ReactiveQuery<R> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Stage.Query<R> setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public Stage.Query<R> setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public <T> Stage.Query<R> setParameter(Parameter<T> parameter, T value) {
		delegate.setParameter( parameter, value );
		return this;
	}

	@Override
	public Stage.Query<R> setMaxResults(int maxResults) {
		delegate.setMaxResults( maxResults );
		return this;
	}

	@Override
	public int getMaxResults() {
		return delegate.getMaxResults();
	}

	@Override
	public Stage.Query<R> setFirstResult(int firstResult) {
		delegate.setFirstResult( firstResult );
		return this;
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
	}

	@Override
	public Stage.Query<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public boolean isReadOnly() {
		return delegate.isReadOnly();
	}

	@Override
	public Stage.Query<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

//	@Override
//	public Stage.Query<R> setHint(String hintName, Object value) {
//		delegate.setQueryHint( hintName, value );
//		return null;
//	}

	@Override
	public Stage.Query<R> setLockMode(LockMode lockMode) {
		delegate.setLockMode( lockMode );
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
	public Stage.Query<R> setFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode(flushMode);
		return this;
	}

	@Override
	public FlushMode getFlushMode() {
		return delegate.getHibernateFlushMode();
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

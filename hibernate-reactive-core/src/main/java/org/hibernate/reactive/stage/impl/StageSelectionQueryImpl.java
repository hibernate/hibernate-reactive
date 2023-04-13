/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.graph.RootGraph;
import org.hibernate.reactive.query.ReactiveSelectionQuery;
import org.hibernate.reactive.stage.Stage.SelectionQuery;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class StageSelectionQueryImpl<T> implements SelectionQuery<T> {
	private final ReactiveSelectionQuery<T> delegate;

	public StageSelectionQueryImpl(ReactiveSelectionQuery<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public int getMaxResults() {
		return delegate.getMaxResults();
	}

	@Override
	public CompletionStage<List<T>> getResultList() {
		return delegate.getReactiveResultList();
	}

	@Override
	public FlushMode getFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public SelectionQuery<T> setFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public SelectionQuery<T> setLockMode(LockMode lockMode) {
		delegate.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public SelectionQuery<T> setPlan(EntityGraph<T> entityGraph) {
		delegate.applyFetchGraph( (RootGraph<?>) entityGraph );
		return this;
	}

	@Override
	public CompletionStage<T> getSingleResult() {
		return delegate.getReactiveSingleResult();
	}

	@Override
	public CompletionStage<T> getSingleResultOrNull() {
		return delegate.getReactiveSingleResultOrNull();
	}

	@Override
	public SelectionQuery<T> setFlushMode(FlushModeType flushMode) {
		delegate.setFlushMode( flushMode );
		return this;
	}

	@Override
	public boolean isReadOnly() {
		return delegate.isReadOnly();
	}

	@Override
	public SelectionQuery<T> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public SelectionQuery<T> setMaxResults(int maxResult) {
		delegate.setMaxResults( maxResult );
		return this;
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
	}

	@Override
	public SelectionQuery<T> setFirstResult(int startPosition) {
		delegate.setFirstResult( startPosition );
		return this;
	}

	@Override
	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	@Override
	public CacheStoreMode getCacheStoreMode() {
		return delegate.getCacheStoreMode();
	}

	@Override
	public CacheRetrieveMode getCacheRetrieveMode() {
		return delegate.getCacheRetrieveMode();
	}

	@Override
	public SelectionQuery<T> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public SelectionQuery<T> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		delegate.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public SelectionQuery<T> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		delegate.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public boolean isCacheable() {
		return delegate.isCacheable();
	}

	@Override
	public SelectionQuery<T> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public String getCacheRegion() {
		return delegate.getCacheRegion();
	}

	@Override
	public SelectionQuery<T> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public SelectionQuery<T> setLockMode(LockModeType lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}

	@Override
	public SelectionQuery<T> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public SelectionQuery<T> setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public SelectionQuery<T> setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public <T1> SelectionQuery<T> setParameter(Parameter<T1> param, T1 value) {
		delegate.setParameter( param, value );
		return this;
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public SelectionQuery<T> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}
}

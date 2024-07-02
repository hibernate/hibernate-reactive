/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.query.Order;
import org.hibernate.query.Page;
import org.hibernate.reactive.mutiny.Mutiny.SelectionQuery;
import org.hibernate.reactive.query.ReactiveSelectionQuery;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;

public class MutinySelectionQueryImpl<R> implements SelectionQuery<R> {
	private final MutinySessionFactoryImpl factory;
	private final ReactiveSelectionQuery<R> delegate;

	public MutinySelectionQueryImpl(ReactiveSelectionQuery<R> delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	private <T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public int getMaxResults() {
		return delegate.getMaxResults();
	}

	@Override
	public Uni<Long> getResultCount() {
		return uni( delegate::getReactiveResultCount );
	}

	@Override
	public Uni<List<R>> getResultList() {
		return uni( delegate::getReactiveResultList );
	}

	@Override
	public FlushMode getFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public SelectionQuery<R> setFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public SelectionQuery<R> setLockMode(LockMode lockMode) {
		delegate.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public SelectionQuery<R> setPlan(EntityGraph<R> entityGraph) {
		delegate.applyGraph( (RootGraphImplementor<?>) entityGraph, GraphSemantic.FETCH );
		return this;
	}

	@Override
	public Uni<R> getSingleResult() {
		return uni( delegate::getReactiveSingleResult );
	}

	@Override
	public Uni<R> getSingleResultOrNull() {
		return uni( delegate::getReactiveSingleResultOrNull );
	}

	@Override
	public SelectionQuery<R> setFlushMode(FlushModeType flushMode) {
		delegate.setFlushMode( flushMode );
		return this;
	}

	@Override
	public boolean isReadOnly() {
		return delegate.isReadOnly();
	}

	@Override
	public SelectionQuery<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public SelectionQuery<R> setMaxResults(int maxResult) {
		delegate.setMaxResults( maxResult );
		return this;
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
	}

	@Override
	public SelectionQuery<R> setFirstResult(int startPosition) {
		delegate.setFirstResult( startPosition );
		return this;
	}

	@Override
	public SelectionQuery<R> setPage(Page page) {
		setMaxResults( page.getMaxResults() );
		setFirstResult( page.getFirstResult() );
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
	public SelectionQuery<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public SelectionQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		delegate.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public SelectionQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		delegate.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public boolean isCacheable() {
		return delegate.isCacheable();
	}

	@Override
	public SelectionQuery<R> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public String getCacheRegion() {
		return delegate.getCacheRegion();
	}

	@Override
	public SelectionQuery<R> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public SelectionQuery<R> setLockMode(LockModeType lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}

	@Override
	public SelectionQuery<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public SelectionQuery<R> setOrder(List<Order<? super R>> orders) {
		delegate.setOrder( orders );
		return this;
	}

	@Override
	public SelectionQuery<R> setOrder(Order<? super R> order) {
		delegate.setOrder( order );
		return this;
	}


	@Override
	public SelectionQuery<R> setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public SelectionQuery<R> setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public <T1> SelectionQuery<R> setParameter(Parameter<T1> param, T1 value) {
		delegate.setParameter( param, value );
		return this;
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public SelectionQuery<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public SelectionQuery<R> enableFetchProfile(String profileName) {
		delegate.enableFetchProfile( profileName );
		return this;
	}
}

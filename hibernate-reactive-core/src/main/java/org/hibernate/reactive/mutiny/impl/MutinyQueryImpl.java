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
import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.reactive.mutiny.Mutiny;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.Parameter;

import static org.hibernate.jpa.internal.util.LockModeTypeHelper.getLockModeType;

/**
 * Implementation of {@link Mutiny.Query}.
 */
public class MutinyQueryImpl<R> implements Mutiny.Query<R> {

	private final QueryImplementor<R> delegate;
	private final MutinySessionFactoryImpl factory;

	public MutinyQueryImpl(QueryImplementor<R> delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}
|
	private <T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public Mutiny.Query<R> setParameter(int parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(String parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <T> Mutiny.Query<R> setParameter(Parameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public Mutiny.Query<R> setMaxResults(int maxResults) {
		return (Mutiny.Query) delegate.setMaxResults( maxResults );
	}

	@Override
	public Mutiny.Query<R> setFirstResult(int firstResult) {
		return (Mutiny.Query) delegate.setFirstResult( firstResult );
	}

	@Override
	public int getMaxResults() {
		return delegate.getMaxResults();
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
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
	public Uni<List<R>> getResultList() {
		return uni( delegate::getReactiveResultList );
	}

	@Override
	public Uni<Integer> executeUpdate() {
		return null;
	}

	@Override
	public Mutiny.Query<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;

	}

	@Override
	public boolean isReadOnly() {
		return delegate.isReadOnly();
	}

	@Override
	public Mutiny.Query<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public boolean isCacheable() {
		return delegate.isCacheable();
	}

	@Override
	public Mutiny.Query<R> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public String getCacheRegion() {
		return delegate.getCacheRegion();
	}

	@Override
	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	@Override
	public Mutiny.Query<R> setFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public Mutiny.Query<R> setFlushMode(FlushModeType flushModeType) {
		delegate.setFlushMode( flushModeType );
		return this;
	}

	@Override
	public FlushModeType getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public Mutiny.Query<R> setLockMode(LockMode lockMode) {
		delegate.setLockMode( getLockModeType( lockMode ) );
		return this;
	}

	@Override
	public Mutiny.Query<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Mutiny.Query<R> setPlan(EntityGraph<R> entityGraph) {
		throw new NotYetImplementedFor6Exception();
	}
}

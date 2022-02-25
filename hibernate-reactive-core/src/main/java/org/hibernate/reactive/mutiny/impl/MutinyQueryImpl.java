/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.session.ReactiveQuery;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.Parameter;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Implementation of {@link Mutiny.Query}.
 */
public class MutinyQueryImpl<R> implements Mutiny.Query<R> {

	private final ReactiveQuery<R> delegate;
	private final MutinySessionFactoryImpl factory;

	public MutinyQueryImpl(ReactiveQuery<R> delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	public MutinyQueryImpl(ReactiveQuery<R> delegate, String[] querySpaces, MutinySessionFactoryImpl factory) {
		this(delegate, factory);
		delegate.setQuerySpaces( querySpaces );
	}

	<T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni(stageSupplier);
	}

    @Override
	public Mutiny.Query<R> setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public <T> Mutiny.Query<R> setParameter(Parameter<T> name, T value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public Mutiny.Query<R> setMaxResults(int maxResults) {
		delegate.setMaxResults( maxResults );
		return this;
	}

	@Override
	public int getMaxResults() {
		return delegate.getMaxResults();
	}

	@Override
	public Mutiny.Query<R> setFirstResult(int firstResult) {
		delegate.setFirstResult( firstResult );
		return this;
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
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

//	@Override
//	public Mutiny.Query<R> setHint(String hintName, Object value) {
//		delegate.setQueryHint( hintName, value );
//		return null;
//	}

	@Override
	public Mutiny.Query<R> setCacheable(boolean cacheable) {
		delegate.setCacheable(cacheable);
		return this;
	}

	@Override
	public boolean isCacheable() {
		return delegate.isCacheable();
	}

	@Override
	public Mutiny.Query<R> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion(cacheRegion);
		return this;
	}

	@Override
	public String getCacheRegion() {
		return delegate.getCacheRegion();
	}

	@Override
	public Mutiny.Query<R> setLockMode(LockMode lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}

//	@Override
	public Mutiny.Query<R> setLockOptions(LockOptions lockOptions) {
		delegate.setLockOptions(lockOptions);
		return this;
	}

	@Override
	public Mutiny.Query<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	@Override
	public Mutiny.Query<R> setFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode(flushMode);
		return this;
	}

	@Override
	public FlushMode getFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public Mutiny.Query<R> setPlan(EntityGraph<R> entityGraph) {
		delegate.setPlan(entityGraph);
		return this;
	}

	@Override
	public Uni<Integer> executeUpdate() {
		return uni( delegate::executeReactiveUpdate );
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

}

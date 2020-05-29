/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.CacheMode;
import org.hibernate.LockMode;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.mutiny.Mutiny;

import javax.persistence.Parameter;
import java.util.List;

/**
 * Implementation of {@link Mutiny.Query}.
 */
public class MutinyQueryImpl<R> implements Mutiny.Query<R> {

	private final ReactiveQuery<R> delegate;

	public MutinyQueryImpl(ReactiveQuery<R> delegate) {
		this.delegate = delegate;
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
	public Mutiny.Query<R> setFirstResult(int firstResult) {
		delegate.setFirstResult( firstResult );
		return this;
	}

	@Override
	public Mutiny.Query<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public Mutiny.Query<R> setComment(String comment) {
		delegate.setComment( comment );
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
	public Uni<Integer> executeUpdate() {
		return Uni.createFrom().completionStage( delegate.executeReactiveUpdate() );
	}

	@Override
	public Uni<R> getSingleResult() {
		return Uni.createFrom().completionStage( delegate.getReactiveSingleResult() );
	}

	@Override
	public Uni<List<R>> getResultList() {
		return Uni.createFrom().completionStage( delegate.getReactiveResultList() );
	}

}

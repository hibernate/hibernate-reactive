/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import java.util.List;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.reactive.mutiny.Mutiny;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.Parameter;

/**
 * Implementation of {@link Mutiny.Query}.
 */
// FIXME: [ORM-6]
public class MutinyQueryImpl<R> implements Mutiny.Query<R> {

	@Override
	public Mutiny.Query<R> setParameter(int parameter, Object argument) {
		return null;
	}

	@Override
	public Mutiny.Query<R> setParameter(String parameter, Object argument) {
		return null;
	}

	@Override
	public <T> Mutiny.Query<R> setParameter(Parameter<T> parameter, T argument) {
		return null;
	}

	@Override
	public Mutiny.Query<R> setMaxResults(int maxResults) {
		return null;
	}

	@Override
	public Mutiny.Query<R> setFirstResult(int firstResult) {
		return null;
	}

	@Override
	public int getMaxResults() {
		return 0;
	}

	@Override
	public int getFirstResult() {
		return 0;
	}

	@Override
	public Uni<R> getSingleResult() {
		return null;
	}

	@Override
	public Uni<R> getSingleResultOrNull() {
		return null;
	}

	@Override
	public Uni<List<R>> getResultList() {
		return null;
	}

	@Override
	public Uni<Integer> executeUpdate() {
		return null;
	}

	@Override
	public Mutiny.Query<R> setReadOnly(boolean readOnly) {
		return null;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public Mutiny.Query<R> setComment(String comment) {
		return null;
	}

	@Override
	public Mutiny.Query<R> setCacheable(boolean cacheable) {
		return null;
	}

	@Override
	public boolean isCacheable() {
		return false;
	}

	@Override
	public Mutiny.Query<R> setCacheRegion(String cacheRegion) {
		return null;
	}

	@Override
	public String getCacheRegion() {
		return null;
	}

	@Override
	public Mutiny.Query<R> setCacheMode(CacheMode cacheMode) {
		return null;
	}

	@Override
	public CacheMode getCacheMode() {
		return null;
	}

	@Override
	public Mutiny.Query<R> setFlushMode(FlushMode flushMode) {
		return null;
	}

	@Override
	public FlushMode getFlushMode() {
		return null;
	}

	@Override
	public Mutiny.Query<R> setLockMode(LockMode lockMode) {
		return null;
	}

	@Override
	public Mutiny.Query<R> setLockMode(String alias, LockMode lockMode) {
		return null;
	}

	@Override
	public Mutiny.Query<R> setPlan(EntityGraph<R> entityGraph) {
		return null;
	}
}

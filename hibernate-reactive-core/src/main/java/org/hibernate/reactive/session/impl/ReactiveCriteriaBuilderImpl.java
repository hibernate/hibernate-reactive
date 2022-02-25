/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;

import jakarta.persistence.Tuple;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;

/**
 * A JPA {@link jakarta.persistence.criteria.CriteriaBuilder} for
 * that acts as a factory for {@link ReactiveCriteriaQueryImpl}.
 *
 * @author Gavin King
 */
public class ReactiveCriteriaBuilderImpl extends CriteriaBuilderImpl {
	public ReactiveCriteriaBuilderImpl(SessionFactoryImpl sessionFactory) {
		super(sessionFactory);
	}

	@Override
	public CriteriaQuery<Object> createQuery() {
		return new ReactiveCriteriaQueryImpl<>( this, Object.class );
	}

	@Override
	public <T> CriteriaQuery<T> createQuery(Class<T> resultClass) {
		return new ReactiveCriteriaQueryImpl<>( this, resultClass );
	}

	@Override
	public <T> CriteriaUpdate<T> createCriteriaUpdate(Class<T> targetEntity) {
		return new ReactiveCriteriaUpdateImpl<>(this);
	}

	@Override
	public <T> CriteriaDelete<T> createCriteriaDelete(Class<T> targetEntity) {
		return new ReactiveCriteriaDeleteImpl<>(this);
	}

	@Override
	public CriteriaQuery<Tuple> createTupleQuery() {
		return new ReactiveCriteriaQueryImpl<>( this, Tuple.class );
	}
}

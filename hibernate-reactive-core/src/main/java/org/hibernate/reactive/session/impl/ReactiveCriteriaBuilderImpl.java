package org.hibernate.reactive.session.impl;

import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaQuery;

/**
 * A JPA {@link javax.persistence.criteria.CriteriaBuilder} for
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
	public CriteriaQuery<Tuple> createTupleQuery() {
		return new ReactiveCriteriaQueryImpl<>( this, Tuple.class );
	}
}

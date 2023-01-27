/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.util.Map;
import java.util.Objects;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.criteria.internal.NamedCriteriaQueryMementoImpl;
import org.hibernate.query.hql.internal.NamedHqlQueryMementoImpl;
import org.hibernate.query.hql.spi.SqmQueryImplementor;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.sqm.SqmSelectionQuery;
import org.hibernate.query.sqm.spi.NamedSqmQueryMemento;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.query.sqm.iternal.ReactiveQuerySqmImpl;
import org.hibernate.reactive.query.sqm.iternal.ReactiveSqmSelectionQueryImpl;

/**
 * @see org.hibernate.query.sql.spi.NamedNativeQueryMemento
 */
public class ReactiveNamedSqmQueryMemento implements NamedSqmQueryMemento {

	private final NamedSqmQueryMemento delegate;

	public ReactiveNamedSqmQueryMemento(NamedSqmQueryMemento delegate) {
		Objects.requireNonNull( delegate );
		this.delegate = delegate;
	}

	@Override
	public <T> SqmQueryImplementor<T> toQuery(SharedSessionContractImplementor session) {
		return toQuery( session, null );
	}

	@Override
	public <T> SqmQueryImplementor<T> toQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		// A bit of a hack, I'm sure that if we have a better look at this we can avoid the instanceof
		if ( delegate instanceof NamedHqlQueryMementoImpl ) {
			return new ReactiveQuerySqmImpl<T>( (NamedHqlQueryMementoImpl) delegate, resultType, session );
		}
		if ( delegate instanceof NamedHqlQueryMementoImpl ) {
			return new ReactiveQuerySqmImpl<T>( (NamedCriteriaQueryMementoImpl) delegate, resultType, session );
		}

		throw new UnsupportedOperationException( "NamedSqmQueryMemento not recognized: " + delegate.getClass() );
	}

	@Override
	public <T> SqmSelectionQuery<T> toSelectionQuery(Class<T> resultType, SharedSessionContractImplementor session) {
		SqmSelectionQuery<T> selectionQuery = delegate.toSelectionQuery( resultType, session );
		return selectionQuery == null
				? null
				: new ReactiveSqmSelectionQueryImpl<>( (SqmSelectStatement) selectionQuery.getSqmStatement(), resultType, session );
	}

	@Override
	public String getHqlString() {
		return delegate.getHqlString();
	}

	@Override
	public SqmStatement getSqmStatement() {
		return delegate.getSqmStatement();
	}

	@Override
	public Integer getFirstResult() {
		return delegate.getFirstResult();
	}

	@Override
	public Integer getMaxResults() {
		return delegate.getMaxResults();
	}

	@Override
	public LockOptions getLockOptions() {
		return delegate.getLockOptions();
	}

	@Override
	public Map<String, String> getParameterTypes() {
		return delegate.getParameterTypes();
	}

	@Override
	public NamedSqmQueryMemento makeCopy(String name) {
		return new ReactiveNamedSqmQueryMemento( delegate.makeCopy( name ) );
	}

	@Override
	public String getRegistrationName() {
		return delegate.getRegistrationName();
	}

	@Override
	public Boolean getCacheable() {
		return delegate.getCacheable();
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
	public FlushMode getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public Boolean getReadOnly() {
		return delegate.getReadOnly();
	}

	@Override
	public Integer getTimeout() {
		return delegate.getTimeout();
	}

	@Override
	public Integer getFetchSize() {
		return delegate.getFetchSize();
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public Map<String, Object> getHints() {
		return delegate.getHints();
	}

	@Override
	public void validate(QueryEngine queryEngine) {
		delegate.validate( queryEngine );
	}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.util.Map;
import java.util.Set;

import org.hibernate.FlushMode;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.named.NamedNativeQueryMemento;
import org.hibernate.query.spi.MutationQueryImplementor;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.SelectionQueryImplementor;
import org.hibernate.query.sql.spi.NativeQueryImplementor;
import org.hibernate.reactive.query.sql.internal.ReactiveNativeQueryImpl;

import jakarta.persistence.Timeout;

/**
 * @see NamedNativeQueryMemento
 */
public class ReactiveNamedNativeQueryMemento<E> implements NamedNativeQueryMemento<E> {

	private final NamedNativeQueryMemento<E> delegate;

	public ReactiveNamedNativeQueryMemento(NamedNativeQueryMemento<E> delegate) {
		this.delegate = delegate;
	}

	public NamedNativeQueryMemento<E> getDelegate() {
		return delegate;
	}

	@Override
	public String getSqlString() {
		return delegate.getSqlString();
	}

	@Override
	public Set<String> getQuerySpaces() {
		return delegate.getQuerySpaces();
	}

	@Override
	public String getResultMappingName() {
		return delegate.getResultMappingName();
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
	public NativeQueryImplementor<E> toQuery(SharedSessionContractImplementor session) {
		return new ReactiveNativeQueryImpl<>( this, session );
	}

	@Override
	public <T> NativeQueryImplementor<T> toQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		return new ReactiveNativeQueryImpl<>( this, resultType, session );
	}

	@Override
	public <T> NativeQueryImplementor<T> toQuery(SharedSessionContractImplementor session, String resultSetMapping) {
		return new ReactiveNativeQueryImpl<>( this, resultSetMapping, session );
	}

	@Override
	public SelectionQueryImplementor<E> toSelectionQuery(SharedSessionContractImplementor session) {
		return (SelectionQueryImplementor<E>) delegate.toSelectionQuery( session );
	}

	@Override
	public <X> SelectionQueryImplementor<X> toSelectionQuery(SharedSessionContractImplementor session, Class<X> resultType) {
		return (SelectionQueryImplementor<X>) delegate.toSelectionQuery( session, resultType );
	}

	@Override
	public MutationQueryImplementor<E> toMutationQuery(SharedSessionContractImplementor session) {
		return delegate.toMutationQuery( session );
	}

	@Override
	public <X> MutationQueryImplementor<X> toMutationQuery(SharedSessionContractImplementor session, Class<X> resultType) {
		return delegate.toMutationQuery( session, resultType );
	}

	@Override
	public NamedNativeQueryMemento<E> makeCopy(String name) {
		return new ReactiveNamedNativeQueryMemento<>( delegate.makeCopy( name ) );
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public String getRegistrationName() {
		return delegate.getRegistrationName();
	}

	@Override
	public FlushMode getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public Timeout getTimeout() {
		return delegate.getTimeout();
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

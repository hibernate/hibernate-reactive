/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.util.Map;
import java.util.Set;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.query.sql.spi.NativeQueryImplementor;
import org.hibernate.reactive.query.sql.internal.ReactiveNativeQueryImpl;

/**
 * @see NamedNativeQueryMemento
 */
public class ReactiveNamedNativeQueryMemento implements NamedNativeQueryMemento {

	private final NamedNativeQueryMemento delegate;

	public ReactiveNamedNativeQueryMemento(NamedNativeQueryMemento delegate) {
		this.delegate = delegate;
	}

	@Override
	public String getSqlString() {
		return delegate.getSqlString();
	}

	@Override
	public String getOriginalSqlString() {
		return delegate.getOriginalSqlString();
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
	public Class<?> getResultMappingClass() {
		return delegate.getResultMappingClass();
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
	public <T> NativeQueryImplementor<T> toQuery(SharedSessionContractImplementor session) {
		return new ReactiveNativeQueryImpl<T>( this, session );
	}

	@Override
	public <T> NativeQueryImplementor<T> toQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		return new ReactiveNativeQueryImpl<T>( this, resultType, session );
	}

	@Override
	public <T> NativeQueryImplementor<T> toQuery(SharedSessionContractImplementor session, String resultSetMapping) {
		return new ReactiveNativeQueryImpl<T>( this, resultSetMapping, session );
	}

	@Override
	public NamedNativeQueryMemento makeCopy(String name) {
		return new ReactiveNamedNativeQueryMemento( delegate.makeCopy( name ) );
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

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.util.Map;
import java.util.Objects;

import org.hibernate.FlushMode;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.named.NamedNativeQueryMemento;
import org.hibernate.query.named.NamedSqmQueryMemento;
import org.hibernate.query.named.internal.SqmSelectionMemento;
import org.hibernate.query.spi.MutationQueryImplementor;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.query.spi.SelectionQueryImplementor;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.query.sqm.internal.ReactiveSqmQueryImpl;
import org.hibernate.reactive.query.sqm.internal.ReactiveSqmSelectionQueryImpl;

import jakarta.persistence.Timeout;

/**
 * @see NamedNativeQueryMemento
 */
public class ReactiveNamedSqmQueryMemento<E> implements NamedSqmQueryMemento<E> {

	private final NamedSqmQueryMemento<E> delegate;

	public ReactiveNamedSqmQueryMemento(NamedSqmQueryMemento<E> delegate) {
		Objects.requireNonNull( delegate );
		this.delegate = delegate;
	}

	@Override
	public QueryImplementor<E> toQuery(SharedSessionContractImplementor session) {
		return toQuery( session, null );
	}

	@Override
	public <T> QueryImplementor<T> toQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		@SuppressWarnings("unchecked")
		final NamedSqmQueryMemento<T> typedDelegate = (NamedSqmQueryMemento<T>) delegate;
		return new ReactiveSqmQueryImpl<>( typedDelegate, resultType, session );
	}

	@Override
	public SelectionQueryImplementor<E> toSelectionQuery(SharedSessionContractImplementor session) {
		return toSelectionQuery( session, null );
	}

	@Override
	public <T> SelectionQueryImplementor<T> toSelectionQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		if ( delegate instanceof SqmSelectionMemento ) {
			final SqmStatement<E> sqmStatement = delegate.getSqmStatement();
			@SuppressWarnings("unchecked")
			final SqmSelectStatement<T> statement = (SqmSelectStatement<T>) sqmStatement;
			return new ReactiveSqmSelectionQueryImpl<>( statement, resultType, session );
		}
		return null;
	}

	@Override
	public MutationQueryImplementor<E> toMutationQuery(SharedSessionContractImplementor session) {
		return delegate.toMutationQuery( session );
	}

	@Override
	public <T> MutationQueryImplementor<T> toMutationQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		return delegate.toMutationQuery( session, resultType );
	}

	@Override
	public String getHqlString() {
		return delegate.getHqlString();
	}

	@Override
	public SqmStatement<E> getSqmStatement() {
		return delegate.getSqmStatement();
	}

	@Override
	public Map<String, String> getAnticipatedParameterTypes() {
		return delegate.getAnticipatedParameterTypes();
	}

	@Override
	public NamedSqmQueryMemento<E> makeCopy(String name) {
		return new ReactiveNamedSqmQueryMemento<>( delegate.makeCopy( name ) );
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

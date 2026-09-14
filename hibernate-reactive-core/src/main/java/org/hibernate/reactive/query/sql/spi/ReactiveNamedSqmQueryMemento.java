/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

import java.util.Map;
import java.util.Objects;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.internal.QueryHelper;
import org.hibernate.query.named.internal.CriteriaMutationMementoImpl;
import org.hibernate.query.named.internal.HqlMutationMementoImpl;
import org.hibernate.query.named.internal.SqmSelectionMemento;
import org.hibernate.query.named.spi.NamedNativeQueryMemento;
import org.hibernate.query.named.spi.NamedSqmQueryMemento;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.MutationQueryImplementor;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.query.spi.SelectionQueryImplementor;
import org.hibernate.query.sqm.tree.spi.SqmDmlStatement;
import org.hibernate.query.sqm.tree.spi.SqmStatement;
import org.hibernate.query.sqm.tree.spi.select.SqmSelectStatement;
import org.hibernate.reactive.query.sqm.internal.ReactiveMutationQueryImpl;
import org.hibernate.reactive.query.sqm.internal.ReactiveSelectionQueryImpl;

import jakarta.persistence.QueryFlushMode;
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
		return toMutationQuery( session, resultType );
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
			return new ReactiveSelectionQueryImpl<>( statement, resultType, session );
		}
		// Delegate is a mutation memento - toSelectionQuery on a mutation query is invalid
		return delegate.toSelectionQuery( session, resultType );
	}

	@Override
	public MutationQueryImplementor<E> toMutationQuery(SharedSessionContractImplementor session) {
		return toMutationQuery( session, null );
	}

	@Override
	public <T> MutationQueryImplementor<T> toMutationQuery(SharedSessionContractImplementor session, Class<T> resultType) {
		@SuppressWarnings("unchecked")
		final NamedSqmQueryMemento<T> typedDelegate = (NamedSqmQueryMemento<T>) delegate;
		if ( typedDelegate instanceof HqlMutationMementoImpl<T> hqlMemento ) {
			final HqlInterpretation<T> interpretation = QueryHelper.interpretation( hqlMemento, resultType, session );
			return new ReactiveMutationQueryImpl<>( hqlMemento, interpretation, resultType, session );
		}
		if ( typedDelegate instanceof CriteriaMutationMementoImpl<T> criteriaMemento ) {
			@SuppressWarnings("unchecked")
			final SqmDmlStatement<T> sqm = (SqmDmlStatement<T>) criteriaMemento.getSqmStatement();
			return new ReactiveMutationQueryImpl<>( criteriaMemento, sqm, session );
		}
		// Fallback: delegate handles its own instantiation (e.g. custom memento implementations)
		return typedDelegate.toMutationQuery( session, resultType );
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
	public String getRegistrationName() {
		return delegate.getRegistrationName();
	}

	@Override
	public QueryFlushMode getQueryFlushMode() {
		return delegate.getQueryFlushMode();
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

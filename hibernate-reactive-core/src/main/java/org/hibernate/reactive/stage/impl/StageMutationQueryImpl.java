/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import jakarta.persistence.Parameter;
import org.hibernate.reactive.query.ReactiveMutationQuery;
import org.hibernate.reactive.stage.Stage.MutationQuery;

import org.hibernate.reactive.engine.impl.InternalStage;

public class StageMutationQueryImpl<T> implements MutationQuery {

	private final ReactiveMutationQuery<T> delegate;

	public StageMutationQueryImpl(ReactiveMutationQuery<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public InternalStage<Integer> executeUpdate() {
		return delegate.executeReactiveUpdate();
	}

	@Override
	public MutationQuery setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public MutationQuery setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public <P> MutationQuery setParameter(Parameter<P> param, P value) {
		delegate.setParameter( param, value );
		return this;
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public MutationQuery setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}
}

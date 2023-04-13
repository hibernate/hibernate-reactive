/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.Parameter;
import org.hibernate.reactive.mutiny.Mutiny.MutationQuery;
import org.hibernate.reactive.query.ReactiveQuery;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class MutinyMutationQueryImpl<R> implements MutationQuery {

	private final MutinySessionFactoryImpl factory;
	private final ReactiveQuery<R> delegate;

	public MutinyMutationQueryImpl(ReactiveQuery<R> delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	private <T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public Uni<Integer> executeUpdate() {
		return uni( delegate::executeReactiveUpdate );
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

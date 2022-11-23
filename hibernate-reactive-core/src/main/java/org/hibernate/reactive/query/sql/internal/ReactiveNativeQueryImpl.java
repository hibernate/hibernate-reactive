/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.internal;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.sql.internal.NativeQueryImpl;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sql.spi.ReactiveNativeQueryImplementor;

public class ReactiveNativeQueryImpl<R> extends NativeQueryImpl<R> implements ReactiveNativeQueryImplementor<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveNativeQueryImpl(
			NamedNativeQueryMemento memento,
			SharedSessionContractImplementor session) {
		super( memento, session );
	}

	@Override
	public R getSingleResult() {
		throw LOG.nonReactiveMethodCall( "reactiveUniqueResult" );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return null;
	}

	@Override
	public Optional<R> uniqueResultOptional() {
		throw LOG.nonReactiveMethodCall( "reactiveUniqueResultOptional" );
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		return null;
	}

	@Override
	public R uniqueResult() {
		throw LOG.nonReactiveMethodCall( "reactiveUniqueResult" );
	}

	@Override
	public CompletionStage<R> reactiveUniqueResult() {
		return null;
	}

	@Override
	public List<R> list() {
		throw LOG.nonReactiveMethodCall( "reactiveList" );
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
		return null;
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import org.hibernate.HibernateException;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveCriteriaBuilderImpl;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.stage.Stage;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Implementation of {@link Stage.SessionFactory}.
 *
 * @see org.hibernate.reactive.boot.impl.ReactiveSessionFactoryBuilder
 */
public class StageSessionFactoryImpl implements Stage.SessionFactory {

	private SessionFactoryImpl delegate;

	public StageSessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
	}

	@Override
	public CompletionStage<Stage.Session> openSession() throws HibernateException {
		ReactiveConnectionPool pool = delegate.getServiceRegistry()
				.getService(ReactiveConnectionPool.class);
		return pool.getConnection()
				.thenApply( reactiveConnection -> new ReactiveSessionImpl(
						delegate,
						new SessionFactoryImpl.SessionBuilderImpl<>(delegate),
						reactiveConnection
				) )
				.thenApply( StageSessionImpl::new );
	}

	@Override
	public <T> CompletionStage<T> withSession(Function<Stage.Session, CompletionStage<T>> work) {
		return openSession().thenCompose(
				session -> work.apply(session).whenComplete( (r, e) -> session.close() )
		);
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		return new ReactiveCriteriaBuilderImpl( delegate );
	}

	@Override
	public Metamodel getMetamodel() {
		return delegate.getMetamodel();
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}
}

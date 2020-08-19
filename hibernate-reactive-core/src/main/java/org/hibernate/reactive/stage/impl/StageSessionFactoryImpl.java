/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import org.hibernate.Cache;
import org.hibernate.HibernateException;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveCriteriaBuilderImpl;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl;
import org.hibernate.reactive.stage.Stage;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation of {@link Stage.SessionFactory}.
 * <p>
 * Obtained by calling {@link org.hibernate.SessionFactory#unwrap(Class)}.
 */
public class StageSessionFactoryImpl implements Stage.SessionFactory {

	private SessionFactoryImpl delegate;

	public StageSessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
	}

	@Override
	public Stage.Session createSession() {
		ReactiveConnectionPool pool = delegate.getServiceRegistry()
				.getService(ReactiveConnectionPool.class);
		return new StageSessionImpl(
				new ReactiveSessionImpl(
						delegate,
						new SessionFactoryImpl.SessionBuilderImpl<>(delegate),
						pool.getProxyConnection()
				)
		);
	}

	public Stage.StatelessSession createStatelessSession() {
		ReactiveConnectionPool pool = delegate.getServiceRegistry()
				.getService(ReactiveConnectionPool.class);
		return new StageStatelessSessionImpl(
				new ReactiveStatelessSessionImpl(
						delegate,
						new SessionFactoryImpl.SessionBuilderImpl<>(delegate),
						pool.getProxyConnection()
				)
		);
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
	public CompletionStage<Stage.StatelessSession> openStatelessSession() throws HibernateException {
		ReactiveConnectionPool pool = delegate.getServiceRegistry()
				.getService(ReactiveConnectionPool.class);
		return pool.getConnection()
				.thenApply( reactiveConnection -> new ReactiveStatelessSessionImpl(
						delegate,
						new SessionFactoryImpl.SessionBuilderImpl<>(delegate),
						reactiveConnection
				) )
				.thenApply( StageStatelessSessionImpl::new );
	}

	@Override
	public <T> CompletionStage<T> withSession(Function<Stage.Session, CompletionStage<T>> work) {
		return openSession().thenCompose(
				session -> work.apply(session).whenComplete( (r, e) -> session.close() )
		);
	}

	@Override
	public <T> CompletionStage<T> withTransaction(BiFunction<Stage.Session, Stage.Transaction, CompletionStage<T>> work) {
		return withSession( (s) -> s.withTransaction( (t) -> work.apply(s, t) ) );
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
	public Cache getCache() {
		return delegate.getCache();
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

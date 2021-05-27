/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;

import org.hibernate.Cache;
import org.hibernate.HibernateException;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveCriteriaBuilderImpl;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hibernate.reactive.common.InternalStateAssertions.assertUseOnEventLoop;

/**
 * Implementation of {@link Mutiny.SessionFactory}.
 * <p>
 * Obtained by calling {@link org.hibernate.SessionFactory#unwrap(Class)}.
 */
public class MutinySessionFactoryImpl implements Mutiny.SessionFactory {

	private final SessionFactoryImpl delegate;
	private final ReactiveConnectionPool connectionPool;
	private final Context context;
	private final String sessionId;
	private final String statelessSessionId;

	public MutinySessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
		context = delegate.getServiceRegistry().getService( Context.class );
		connectionPool = delegate.getServiceRegistry().getService( ReactiveConnectionPool.class );
		sessionId = Mutiny.Session.class.getName() + '/' + delegate.getUuid();
		statelessSessionId = Mutiny.StatelessSession.class.getName() + '/' + delegate.getUuid();
	}

	<T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return Uni.createFrom().completionStage(stageSupplier).runSubscriptionOn( context );
	}

	@Override
	public Mutiny.Session openSession() {
		SessionCreationOptions options = options();
		return new MutinySessionImpl(
				new ReactiveSessionImpl( delegate, options, proxyConnection( options.getTenantIdentifier() ) ),
				this
		);
	}

	@Override
	public Mutiny.Session openSession(String tenantId) {
		return new MutinySessionImpl(
				new ReactiveSessionImpl( delegate, options( tenantId ), proxyConnection( tenantId ) ),
				this
		);
	}

	Uni<Mutiny.Session> newSession() throws HibernateException {
		SessionCreationOptions options = options();
		return uni( () -> connection( options.getTenantIdentifier() ) )
				.map( reactiveConnection -> new ReactiveSessionImpl( delegate, options, reactiveConnection ) )
				.map( s -> new MutinySessionImpl(s, this) );
	}

	Uni<Mutiny.Session> newSession(String tenantId) throws HibernateException {
		return uni( () -> connection( tenantId ) )
				.map( reactiveConnection -> new ReactiveSessionImpl( delegate, options( tenantId ), reactiveConnection ) )
				.map( s -> new MutinySessionImpl(s, this) );
	}

	@Override
	public Mutiny.StatelessSession openStatelessSession() {
		SessionCreationOptions options = options();
		return new MutinyStatelessSessionImpl(
				new ReactiveStatelessSessionImpl( delegate, options, proxyConnection( options.getTenantIdentifier() ) ),
				this
		);
	}

	Uni<Mutiny.StatelessSession> newStatelessSession() throws HibernateException {
		SessionCreationOptions options = options();
		return uni( () -> connection( options.getTenantIdentifier() ) )
				.map( reactiveConnection -> new ReactiveStatelessSessionImpl( delegate, options, reactiveConnection ) )
				.map( s -> new MutinyStatelessSessionImpl(s, this) );
	}

	private SessionCreationOptions options() {
		return new SessionFactoryImpl.SessionBuilderImpl<>( delegate );
	}

	private SessionCreationOptions options(String tenantIdentifier) {
		return (SessionCreationOptions) new SessionFactoryImpl.SessionBuilderImpl<>( delegate )
				.tenantIdentifier( tenantIdentifier );
	}

	private CompletionStage<ReactiveConnection> connection(String tenantId) {
		assertUseOnEventLoop();
		return tenantId == null
				? connectionPool.getConnection()
				: connectionPool.getConnection( tenantId );
	}

	private ReactiveConnection proxyConnection(String tenantId) {
		assertUseOnEventLoop();
		return tenantId==null
				? connectionPool.getProxyConnection()
				: connectionPool.getProxyConnection( tenantId );
	}

	@Override
	public <T> Uni<T> withSession(Function<Mutiny.Session, Uni<T>> work) {
		Mutiny.Session current = context.get(Mutiny.Session.class, sessionId);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newSession()
				.chain( session -> {
					context.put(Mutiny.Session.class, sessionId, session);
					return work.apply( session )
							.eventually( () -> {
								context.remove(Mutiny.Session.class, sessionId);
								return session.close();
							} );
				} );
	}

	@Override
	public <T> Uni<T> withSession(String tenantId, Function<Mutiny.Session, Uni<T>> work) {
		String id = sessionId + '/' + tenantId;
		Mutiny.Session current = context.get(Mutiny.Session.class, id);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newSession( tenantId )
				.chain( session -> {
					context.put(Mutiny.Session.class, id, session);
					return work.apply(session)
							.eventually( () -> {
								context.remove(Mutiny.Session.class, id);
								return session.close();
							} );
				} );
	}

	@Override
	public <T> Uni<T> withStatelessSession(Function<Mutiny.StatelessSession, Uni<T>> work) {
		Mutiny.StatelessSession current = context.get(Mutiny.StatelessSession.class, statelessSessionId);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newStatelessSession()
				.chain( session -> {
					context.put(Mutiny.StatelessSession.class, statelessSessionId, session);
					return work.apply(session).eventually( () -> {
						context.remove(Mutiny.StatelessSession.class, statelessSessionId);
						return session.close();
					} );
				} );
	}

	@Override
	public <T> Uni<T> withTransaction(BiFunction<Mutiny.Session, Mutiny.Transaction, Uni<T>> work) {
		return withSession( s -> s.withTransaction( t -> work.apply(s, t) ) );
	}

	@Override
	public <T> Uni<T> withTransaction(String tenantId, BiFunction<Mutiny.Session, Mutiny.Transaction, Uni<T>> work) {
		return withSession( tenantId, s -> s.withTransaction( t -> work.apply(s, t) ) );
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

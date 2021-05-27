/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;

import org.hibernate.Cache;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveCriteriaBuilderImpl;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl;
import org.hibernate.reactive.stage.Stage;

import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Implementation of {@link Stage.SessionFactory}.
 * <p>
 * Obtained by calling {@link org.hibernate.SessionFactory#unwrap(Class)}.
 */
public class StageSessionFactoryImpl implements Stage.SessionFactory {

	private final SessionFactoryImpl delegate;
	private final ReactiveConnectionPool connectionPool;
	private final Context context;
	private final String sessionId;
	private final String statelessSessionId;

	public StageSessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
		context = delegate.getServiceRegistry().getService( Context.class );
		connectionPool = delegate.getServiceRegistry().getService( ReactiveConnectionPool.class );
		sessionId = Stage.Session.class.getName() + '/' + delegate.getUuid();
		statelessSessionId = Stage.StatelessSession.class.getName() + '/' + delegate.getUuid();
	}

	<T> CompletionStage<T> stage(Function<Void, CompletionStage<T>> stageSupplier) {
		return voidFuture().thenComposeAsync( stageSupplier, context );
	}

	@Override
	public Stage.Session openSession() {
		SessionCreationOptions options = options();
		return new StageSessionImpl(
				new ReactiveSessionImpl( delegate, options, proxyConnection( options.getTenantIdentifier() ) ),
				this
		);
	}

	@Override
	public Stage.Session openSession(String tenantId) {
		return new StageSessionImpl(
				new ReactiveSessionImpl( delegate, options( tenantId ), proxyConnection( tenantId ) ),
				this
		);
	}

	public Stage.StatelessSession openStatelessSession() {
		SessionCreationOptions options = options();
		return new StageStatelessSessionImpl(
				new ReactiveStatelessSessionImpl( delegate, options, proxyConnection( options.getTenantIdentifier() ) ),
				this
		);
	}

	CompletionStage<Stage.Session> newSession() {
		SessionCreationOptions options = options();
		return stage( v -> connection( options.getTenantIdentifier() )
				.thenApply( connection -> new ReactiveSessionImpl( delegate, options, connection ) )
				.thenApply( s -> new StageSessionImpl(s, this) ) );
	}

	CompletionStage<Stage.Session> newSession(String tenantId) {
		return stage( v -> connection( tenantId )
				.thenApply( connection -> new ReactiveSessionImpl( delegate, options( tenantId ), connection ) )
				.thenApply( s -> new StageSessionImpl(s, this) ) );
	}

	CompletionStage<Stage.StatelessSession> newStatelessSession() {
		SessionCreationOptions options = options();
		return stage( v -> connection( options.getTenantIdentifier() )
				.thenApply( connection -> new ReactiveStatelessSessionImpl( delegate, options, connection ) )
				.thenApply( s -> new StageStatelessSessionImpl(s, this) ) );
	}

	private SessionCreationOptions options() {
		return new SessionFactoryImpl.SessionBuilderImpl<>( delegate );
	}

	private SessionCreationOptions options(String tenantIdentifier) {
		return (SessionCreationOptions) new SessionFactoryImpl.SessionBuilderImpl<>( delegate )
				.tenantIdentifier( tenantIdentifier );
	}

	private CompletionStage<ReactiveConnection> connection(String tenantId) {
		return tenantId == null
				? connectionPool.getConnection()
				: connectionPool.getConnection( tenantId );
	}

	private ReactiveConnection proxyConnection(String tenantId) {
		return tenantId==null
				? connectionPool.getProxyConnection()
				: connectionPool.getProxyConnection( tenantId );
	}
	@Override
	public <T> CompletionStage<T> withSession(Function<Stage.Session, CompletionStage<T>> work) {
		Stage.Session current = context.get(Stage.Session.class, sessionId);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newSession().thenCompose(
				session -> {
					context.put(Stage.Session.class, sessionId, session);
					return work.apply(session)
							.handle(this::handler)
							.thenCompose( handler -> {
						        context.remove(Stage.Session.class, sessionId);
								return session.close().thenApply(handler);
							} );
				}
		);
	}

	@Override
	public <T> CompletionStage<T> withSession(String tenantId, Function<Stage.Session, CompletionStage<T>> work) {
		String id = sessionId + '/' + tenantId;
		Stage.Session current = context.get(Stage.Session.class, id);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newSession( tenantId ).thenCompose(
				session -> {
					context.put(Stage.Session.class, id, session);
					return work.apply(session)
							.handle(this::handler)
							.thenCompose( handler -> {
						        context.remove(Stage.Session.class, id);
								return session.close().thenApply(handler);
							} );
				}
		);
	}

	@Override
	public <T> CompletionStage<T> withStatelessSession(Function<Stage.StatelessSession, CompletionStage<T>> work) {
		Stage.StatelessSession current = context.get(Stage.StatelessSession.class, statelessSessionId);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newStatelessSession().thenCompose(
				session -> {
					context.put(Stage.StatelessSession.class, statelessSessionId, session);
					return work.apply(session)
							.handle(this::handler)
							.thenCompose( handler -> {
						        context.remove(Stage.StatelessSession.class, statelessSessionId);
								return session.close().thenApply(handler);
							} );
				}
		);
	}

	private <T> Function<Void, T> handler(T result, Throwable exception) {
		return exception == null ? v -> result : v -> rethrow(exception);
	}

	@Override
	public <T> CompletionStage<T> withTransaction(BiFunction<Stage.Session, Stage.Transaction, CompletionStage<T>> work) {
		return withSession( s -> s.withTransaction( t -> work.apply(s, t) ) );
	}

	@Override
	public <T> CompletionStage<T> withTransaction(String tenantId, BiFunction<Stage.Session, Stage.Transaction, CompletionStage<T>> work) {
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

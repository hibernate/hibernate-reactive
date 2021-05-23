/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.hibernate.Cache;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveCriteriaBuilderImpl;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.vertx.VertxInstance;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Implementation of {@link Stage.SessionFactory}.
 * <p>
 * Obtained by calling {@link org.hibernate.SessionFactory#unwrap(Class)}.
 */
public class StageSessionFactoryImpl implements Stage.SessionFactory {

	private final SessionFactoryImpl delegate;
	private final VertxInstance vertxInstance;
	private final Executor vertxExecutor;
	private final ReactiveConnectionPool connectionPool;

	public StageSessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
		this.vertxInstance = delegate.getServiceRegistry().getService( VertxInstance.class );
		this.connectionPool = delegate.getServiceRegistry().getService( ReactiveConnectionPool.class );
		this.vertxExecutor = runnable -> {
			Context context = vertxInstance.getVertx().getOrCreateContext();
			if ( Vertx.currentContext() == context ) {
				runnable.run();
			}
			else {
				context.runOnContext( x -> runnable.run() );
			}
		};
	}

	<T> CompletionStage<T> stage(Function<Void, CompletionStage<T>> stageSupplier) {
		return voidFuture().thenComposeAsync(stageSupplier, vertxExecutor);
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
		String id = sessionId();
		Stage.Session current = Vertx.currentContext().getLocal(id);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newSession().thenCompose(
				session -> {
					Vertx.currentContext().putLocal(id, session);
					return work.apply(session).whenComplete( (r, e) -> {
						Vertx.currentContext().removeLocal(id);
						session.close();
					} );
				}
		);
	}

	@Override
	public <T> CompletionStage<T> withSession(String tenantId, Function<Stage.Session, CompletionStage<T>> work) {
		String id = sessionId(tenantId);
		Stage.Session current = Vertx.currentContext().getLocal(id);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newSession( tenantId ).thenCompose(
				session -> {
					Vertx.currentContext().putLocal(id, session);
					return work.apply(session).whenComplete( (r, e) -> {
						Vertx.currentContext().removeLocal(id);
						session.close();
					} );
				}
		);
	}

	@Override
	public <T> CompletionStage<T> withStatelessSession(Function<Stage.StatelessSession, CompletionStage<T>> work) {
		String id = statelessSessionId();
		Stage.StatelessSession current = Vertx.currentContext().getLocal(id);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return newStatelessSession().thenCompose(
				session -> {
					Vertx.currentContext().putLocal(id, session);
					return work.apply(session).whenComplete( (r, e) -> {
						Vertx.currentContext().removeLocal(id);
						session.close();
					} );
				}
		);
	}

	private String sessionId() {
		return Stage.Session.class.getName() + '/' + delegate.getUuid();
	}

	private String sessionId(String tenantId) {
		return sessionId() + '/' + tenantId;
	}

	private String statelessSessionId() {
		return Stage.StatelessSession.class.getName() + '/' + delegate.getUuid();
	}

	private String statelessSessionId(String tenantId) {
		return statelessSessionId() + '/' + tenantId;
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

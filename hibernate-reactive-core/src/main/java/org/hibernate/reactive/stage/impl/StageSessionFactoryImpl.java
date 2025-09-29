/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.Cache;
import org.hibernate.engine.creation.internal.SessionBuilderImpl;
import org.hibernate.engine.creation.internal.SessionCreationOptions;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionImpl;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.reactive.common.spi.Implementor;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.context.impl.BaseKey;
import org.hibernate.reactive.context.impl.MultitenantKey;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.stat.Statistics;

import jakarta.persistence.metamodel.Metamodel;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.rethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Implementation of {@link Stage.SessionFactory}.
 * <p>
 * Obtained by calling {@link org.hibernate.SessionFactory#unwrap(Class)}.
 */
public class StageSessionFactoryImpl implements Stage.SessionFactory, Implementor {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final SessionFactoryImpl delegate;
	private final ReactiveConnectionPool connectionPool;
	private final Context context;
	private final BaseKey<Stage.Session> contextKeyForSession;
	private final BaseKey<Stage.StatelessSession> contextKeyForStatelessSession;

	public StageSessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
		context = delegate.getServiceRegistry().getService( Context.class );
		connectionPool = delegate.getServiceRegistry().getService( ReactiveConnectionPool.class );
		contextKeyForSession = new BaseKey<>( Stage.Session.class, delegate.getUuid() );
		contextKeyForStatelessSession = new BaseKey<>( Stage.StatelessSession.class, delegate.getUuid() );
	}

	@Override
	public String getUuid() {
		return delegate.getUuid();
	}

	@Override
	public ServiceRegistry getServiceRegistry() {
		return delegate.getServiceRegistry();
	}

	@Override
	public Context getContext() {
		return context;
	}

	@Override
	public CompletionStage<Stage.Session> openSession() {
		SessionCreationOptions options = options();
		return connection( getTenantIdentifier( options ) )
				.thenCompose( connection -> create( connection,
						() -> new ReactiveSessionImpl( delegate, options, connection ) ) )
				.thenApply( StageSessionImpl::new );
	}

	@Override
	public CompletionStage<Stage.Session> openSession(String tenantId) {
		return connection( tenantId )
				.thenCompose( connection -> create( connection,
						() -> new ReactiveSessionImpl( delegate, options( tenantId ), connection ) ) )
				.thenApply( StageSessionImpl::new );
	}

	@Override
	public CompletionStage<Stage.StatelessSession> openStatelessSession() {
		SessionCreationOptions options = options();
		return connection( getTenantIdentifier( options ) )
				.thenCompose( connection -> create( connection,
						() -> new ReactiveStatelessSessionImpl( delegate, options, connection ) ) )
				.thenApply( StageStatelessSessionImpl::new );
	}

	@Override
	public CompletionStage<Stage.StatelessSession> openStatelessSession(String tenantId) {
		return connection( tenantId )
				.thenCompose( connection -> create( connection,
						() -> new ReactiveStatelessSessionImpl( delegate, options( tenantId ), connection ) ) )
				.thenApply( StageStatelessSessionImpl::new );
	}

	/**
	 * Close the connection if something goes wrong during the creation of the session
	 */
	private <S> CompletionStage<S> create(ReactiveConnection connection, Supplier<S> supplier) {
		try {
			return completedFuture( supplier.get() );
		}
		catch (Throwable throwable) {
			return connection.close()
					.handle( this::handler )
					// Ignore exceptions during the connection.close and
					// rethrow the original exception
					.handle( (ignore, t) -> rethrow( throwable ) );
		}
	}

	private SessionCreationOptions options() {
		return new SessionBuilderImpl( delegate ) {
			@Override
			protected SessionImplementor createSession() {
				return new SessionImpl( delegate, this );
			}
		};
	}

	private SessionCreationOptions options(String tenantId) {
		return new SessionBuilderImpl( delegate ) {
			@Override
			protected SessionImplementor createSession() {
				return new SessionImpl( delegate, this );
			}

			@Override
			public Object getTenantIdentifierValue() {
				return tenantId;
			}
		};
	}

	private CompletionStage<ReactiveConnection> connection(String tenantId) {
		return tenantId == null
				? connectionPool.getConnection()
				: connectionPool.getConnection( tenantId );
	}

	@Override
	public Stage.Session getCurrentSession() {
		return context.get( contextKeyForSession );
	}

	@Override
	public Stage.StatelessSession getCurrentStatelessSession() {
		return context.get( contextKeyForStatelessSession );
	}

	@Override
	public <T> CompletionStage<T> withSession(Function<Stage.Session, CompletionStage<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Stage.Session current = context.get( contextKeyForSession );
		if ( current != null && current.isOpen() ) {
			LOG.debug( "Reusing existing open Stage.Session which was found in the current Vert.x context" );
			return work.apply( current );
		}
		else {
			LOG.debug( "No existing open Stage.Session was found in the current Vert.x context: opening a new instance" );
			return executeInContext( v -> withSession( openSession(), work, contextKeyForSession ) );
		}
	}

	@Override
	public <T> CompletionStage<T> withSession(String tenantId, Function<Stage.Session, CompletionStage<T>> work) {
		Objects.requireNonNull( tenantId, "parameter 'tenantId' is required" );
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Context.Key<Stage.Session> key = new MultitenantKey<>( this.contextKeyForSession, tenantId );
		Stage.Session current = context.get( key );
		if ( current != null && current.isOpen() ) {
			LOG.debugf( "Reusing existing open Stage.Session which was found in the current Vert.x context for current tenant '%s'", tenantId );
			return work.apply( current );
		}
		else {
			LOG.debugf( "No existing open Stage.Session was found in the current Vert.x context for current tenant '%s': opening a new instance", tenantId );
			return executeInContext( v -> withSession( openSession( tenantId ), work, key ) );
		}
	}

	@Override
	public <T> CompletionStage<T> withStatelessSession(Function<Stage.StatelessSession, CompletionStage<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Stage.StatelessSession current = context.get( contextKeyForStatelessSession );
		if ( current!=null && current.isOpen() ) {
			LOG.debug( "Reusing existing open Stage.StatelessSession which was found in the current Vert.x context" );
			return work.apply( current );
		}
		else {
			LOG.debug( "No existing open Stage.StatelessSession was found in the current Vert.x context: opening a new instance" );
			return executeInContext( v -> withSession( openStatelessSession(), work, contextKeyForStatelessSession ) );
		}
	}

	@Override
	public <T> CompletionStage<T> withStatelessSession(String tenantId, Function<Stage.StatelessSession, CompletionStage<T>> work) {
		Objects.requireNonNull( tenantId, "parameter 'tenantId' is required" );
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Context.Key<Stage.StatelessSession> key = new MultitenantKey<>( this.contextKeyForStatelessSession, tenantId );
		Stage.StatelessSession current = context.get( key );
		if ( current != null && current.isOpen() ) {
			LOG.debugf( "Reusing existing open Stage.StatelessSession which was found in the current Vert.x context for current tenant '%s'", tenantId );
			return work.apply( current );
		}
		else {
			LOG.debugf( "No existing open Stage.StatelessSession was found in the current Vert.x context for current tenant '%s': opening a new instance", tenantId );
			return executeInContext( v -> withSession( openStatelessSession( tenantId ), work, contextKeyForStatelessSession ) );
		}
	}

	private <T> CompletionStage<T> executeInContext(Function<Void, CompletionStage<T>> fun) {
		return voidFuture().thenComposeAsync( fun, context );
	}

	private <S extends Stage.Closeable, T> CompletionStage<T> withSession(
			CompletionStage<S> sessionStage,
			Function<S, CompletionStage<T>> work,
			Context.Key<S> contextKey) {
		return sessionStage.thenCompose( session -> {
			context.put( contextKey, session );
			return voidFuture()
					// We call work.apply inside a thenCompose so that we can catch all the exceptions
					// and still be able to close the session in case of errors
					.thenCompose( v -> work.apply( session ) )
					.handle( this::handler )
					.thenCompose( handler -> {
						context.remove( contextKey );
						return session.close()
								// Using .handle (instead of .thenApply(handler) because
								// I want to rethrow the original exception in case an error
								// occurs while closing the session
								.handle( (unused, throwable) -> handler.apply( null ) );
					} );
		} );
	}

	private <T> Function<Void, T> handler(T result, Throwable exception) {
		return exception == null ? v -> result : v -> rethrow(exception);
	}

	@Override
	public <T> CompletionStage<T> withTransaction(BiFunction<Stage.Session, Stage.Transaction, CompletionStage<T>> work) {
		return withSession( s -> s.withTransaction( t -> work.apply(s, t) ) );
	}

	@Override
	public <T> CompletionStage<T> withStatelessTransaction(BiFunction<Stage.StatelessSession, Stage.Transaction, CompletionStage<T>> work) {
		return withStatelessSession( s -> s.withTransaction( t -> work.apply(s, t) ) );
	}

	@Override
	public <T> CompletionStage<T> withTransaction(String tenantId, BiFunction<Stage.Session, Stage.Transaction, CompletionStage<T>> work) {
		return withSession( tenantId, s -> s.withTransaction( t -> work.apply(s, t) ) );
	}

	@Override
	public <T> CompletionStage<T> withStatelessTransaction(String tenantId, BiFunction<Stage.StatelessSession, Stage.Transaction, CompletionStage<T>> work) {
		return withStatelessSession( tenantId, s -> s.withTransaction( t -> work.apply( s, t ) ) );
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
	public Statistics getStatistics() {
		return delegate.getStatistics();
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public HibernateCriteriaBuilder getCriteriaBuilder() {
		return delegate.getCriteriaBuilder();
	}

	private String getTenantIdentifier(SessionCreationOptions options) {
		return options.getTenantIdentifierValue() == null ? null : delegate.getTenantIdentifierJavaType().toString(
				options.getTenantIdentifierValue() );
	}
}

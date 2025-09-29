/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

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
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.stat.Statistics;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.metamodel.Metamodel;

import static org.hibernate.reactive.common.InternalStateAssertions.assertUseOnEventLoop;

/**
 * Implementation of {@link Mutiny.SessionFactory}.
 * <p>
 * Obtained by calling {@link org.hibernate.SessionFactory#unwrap(Class)}.
 */
public class MutinySessionFactoryImpl implements Mutiny.SessionFactory, Implementor {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final SessionFactoryImpl delegate;
	private final ReactiveConnectionPool connectionPool;
	private final Context context;

	/**
	 * We store the current sessions in the Context for simplified use;
	 * these are the constant keys, including the SessionFactory's UUID
	 * for correct scoping.
	 * In case of multi-tenancy these will need to be used as prefixes.
	 */
	private final BaseKey<Mutiny.Session> contextKeyForSession;
	private final BaseKey<Mutiny.StatelessSession> contextKeyForStatelessSession;

	public MutinySessionFactoryImpl(SessionFactoryImpl delegate) {
		Objects.requireNonNull( delegate );
		this.delegate = delegate;
		context = delegate.getServiceRegistry().getService( Context.class );
		connectionPool = delegate.getServiceRegistry().getService( ReactiveConnectionPool.class );
		contextKeyForSession = new BaseKey<>( Mutiny.Session.class, delegate.getUuid() );
		contextKeyForStatelessSession = new BaseKey<>( Mutiny.StatelessSession.class, delegate.getUuid() );
	}

	<T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return Uni.createFrom().completionStage( stageSupplier ).runSubscriptionOn( context );
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
	public Uni<Mutiny.Session> openSession() {
		SessionCreationOptions options = options();
		return uni( () -> connection( getTenantIdentifier( options ) ) )
				.chain( reactiveConnection -> create(
						reactiveConnection,
						() -> new ReactiveSessionImpl( delegate, options, reactiveConnection )
				) )
				.map( s -> new MutinySessionImpl( s, this ) );
	}

	@Override
	public Uni<Mutiny.Session> openSession(String tenantId) {
		return uni( () -> connection( tenantId ) )
				.chain( reactiveConnection -> create( reactiveConnection, () -> new ReactiveSessionImpl( delegate, options( tenantId ), reactiveConnection ) ) )
				.map( s -> new MutinySessionImpl( s, this ) );
	}

	/**
	 * Close the connection if something goes wrong during the creation of the session
	 */
	private <S> Uni<S> create(ReactiveConnection connection, Supplier<S> supplier) {
		return Uni.createFrom().item( supplier )
				.onCancellation().call( () -> close( connection ) )
				.onFailure().call( () -> close( connection ) );
	}

	private static Uni<Void> close(ReactiveConnection connection) {
		return Uni.createFrom().completionStage( connection.close() );
	}

	@Override
	public Uni<Mutiny.StatelessSession> openStatelessSession() {
		SessionCreationOptions options = options();
		return uni( () -> connection( getTenantIdentifier( options ) ) )
				.chain( reactiveConnection -> create(
						reactiveConnection,
						() -> new ReactiveStatelessSessionImpl( delegate, options, reactiveConnection )
				) )
				.map( s -> new MutinyStatelessSessionImpl( s, this ) );
	}

	@Override
	public Uni<Mutiny.StatelessSession> openStatelessSession(String tenantId) {
		return uni( () -> connection( tenantId ) )
				.chain( reactiveConnection -> create(
						reactiveConnection,
						() -> new ReactiveStatelessSessionImpl( delegate, options( tenantId ), reactiveConnection )
				) )
				.map( s -> new MutinyStatelessSessionImpl( s, this ) );
	}

	private SessionCreationOptions options() {
		return new SessionBuilderImpl( delegate ) {
			@Override
			protected SessionImplementor createSession() {
				return new SessionImpl( delegate, this );
			}
		};
	}

	private SessionCreationOptions options(String tenantIdentifier) {
		SessionBuilderImpl sessionBuilder = new SessionBuilderImpl( delegate ) {
			@Override
			protected SessionImplementor createSession() {
				return new SessionImpl( delegate, this );
			}
		};
		return (SessionCreationOptions) sessionBuilder.tenantIdentifier( tenantIdentifier );
	}

	private CompletionStage<ReactiveConnection> connection(String tenantId) {
		assertUseOnEventLoop();
		return tenantId == null
				? connectionPool.getConnection()
				: connectionPool.getConnection( tenantId );
	}

	@Override
	public Mutiny.Session getCurrentSession() {
		return context.get( contextKeyForSession );
	}

	@Override
	public Mutiny.StatelessSession getCurrentStatelessSession() {
		return context.get( contextKeyForStatelessSession );
	}

	@Override
	public <T> Uni<T> withSession(Function<Mutiny.Session, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Mutiny.Session current = context.get( contextKeyForSession );
		if ( current != null && current.isOpen() ) {
			LOG.debug( "Reusing existing open Mutiny.Session which was found in the current Vert.x context" );
			return work.apply( current );
		}
		else {
			LOG.debug( "No existing open Mutiny.Session was found in the current Vert.x context: opening a new instance" );
			return withSession( openSession(), work, contextKeyForSession );
		}
	}

	@Override
	public <T> Uni<T> withSession(String tenantId, Function<Mutiny.Session, Uni<T>> work) {
		Objects.requireNonNull( tenantId, "parameter 'tenantId' is required" );
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Context.Key<Mutiny.Session> key = new MultitenantKey<>( contextKeyForSession, tenantId );
		Mutiny.Session current = context.get( key );
		if ( current != null && current.isOpen() ) {
			LOG.debugf( "Reusing existing open Mutiny.Session which was found in the current Vert.x context for current tenant '%s'", tenantId );
			return work.apply( current );
		}
		else {
			LOG.debugf( "No existing open Mutiny.Session was found in the current Vert.x context for current tenant '%s': opening a new instance", tenantId );
			return withSession( openSession( tenantId ), work, key );
		}
	}

	@Override
	public <T> Uni<T> withStatelessSession(Function<Mutiny.StatelessSession, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Mutiny.StatelessSession current = context.get( contextKeyForStatelessSession );
		if ( current != null && current.isOpen() ) {
			LOG.debug( "Reusing existing open Mutiny.StatelessSession which was found in the current Vert.x context" );
			return work.apply( current );
		}
		else {
			LOG.debug( "No existing open Mutiny.StatelessSession was found in the current Vert.x context: opening a new instance" );
			return withSession( openStatelessSession(), work, contextKeyForStatelessSession );
		}
	}

	@Override
	public <T> Uni<T> withStatelessSession(String tenantId, Function<Mutiny.StatelessSession, Uni<T>> work) {
		Objects.requireNonNull( tenantId, "parameter 'tenantId' is required" );
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Context.Key<Mutiny.StatelessSession> key = new MultitenantKey<>( this.contextKeyForStatelessSession, tenantId );
		Mutiny.StatelessSession current = context.get( key );
		if ( current != null && current.isOpen() ) {
			LOG.debugf( "Reusing existing open Mutiny.StatelessSession which was found in the current Vert.x context for current tenant '%s'", tenantId );
			return work.apply( current );
		}
		else {
			LOG.debugf( "No existing open Mutiny.StatelessSession was found in the current Vert.x context for current tenant '%s': opening a new instance", tenantId );
			return withSession( openStatelessSession( tenantId ), work, key );
		}
	}

	private <S extends Mutiny.Closeable, T> Uni<T> withSession(
			Uni<S> sessionUni,
			Function<S, Uni<T>> work,
			Context.Key<S> contextKey) {
		return sessionUni.chain( session -> Uni.createFrom().voidItem()
				.invoke( () -> context.put( contextKey, session ) )
				.chain( () -> work.apply( session ) )
				.onTermination().invoke( () -> context.remove( contextKey ) )
				.onTermination().call( session::close )
		);
	}

	@Override
	public <T> Uni<T> withTransaction(BiFunction<Mutiny.Session, Mutiny.Transaction, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		return withSession( s -> s.withTransaction( t -> work.apply( s, t ) ) );
	}

	@Override
	public <T> Uni<T> withStatelessTransaction(BiFunction<Mutiny.StatelessSession, Mutiny.Transaction, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		return withStatelessSession( s -> s.withTransaction( t -> work.apply( s, t ) ) );
	}

	@Override
	public <T> Uni<T> withTransaction(String tenantId, BiFunction<Mutiny.Session, Mutiny.Transaction, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		return withSession( tenantId, s -> s.withTransaction( t -> work.apply( s, t ) ) );
	}

	@Override
	public <T> Uni<T> withStatelessTransaction(String tenantId, BiFunction<Mutiny.StatelessSession, Mutiny.Transaction, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		return withStatelessSession( tenantId, s -> s.withTransaction( t -> work.apply( s, t ) ) );
	}

	@Override
	public HibernateCriteriaBuilder getCriteriaBuilder() {
		return delegate.getCriteriaBuilder();
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

	private String getTenantIdentifier(SessionCreationOptions options) {
		return options.getTenantIdentifierValue() == null ? null : delegate.getTenantIdentifierJavaType().toString(
				options.getTenantIdentifierValue() );
	}
}

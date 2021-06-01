/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;

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

import io.smallrye.mutiny.Uni;

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

	/**
	 * We store the current sessions in the Context for simplified use;
	 * these are the constant keys, including the SessionFactory's UUID
	 * for correct scoping.
	 * In case of multi-tenancy these will need to be used as prefixes.
	 */
	private final String contextKeyForSession;
	private final String contextKeyForStatelessSession;

	public MutinySessionFactoryImpl(SessionFactoryImpl delegate) {
		Objects.requireNonNull( delegate );
		this.delegate = delegate;
		context = delegate.getServiceRegistry().getService( Context.class );
		connectionPool = delegate.getServiceRegistry().getService( ReactiveConnectionPool.class );
		contextKeyForSession = Mutiny.Session.class.getName() + '/' + delegate.getUuid();
		contextKeyForStatelessSession = Mutiny.StatelessSession.class.getName() + '/' + delegate.getUuid();
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
		Objects.requireNonNull( tenantId, "parameter 'tenantId' is required" );
		return new MutinySessionImpl(
				new ReactiveSessionImpl( delegate, options( tenantId ), proxyConnection( tenantId ) ),
				this
		);
	}

	Uni<Mutiny.Session> newSession() throws HibernateException {
		SessionCreationOptions options = options();
		return uni( () -> connection( options.getTenantIdentifier() ) )
				.chain( reactiveConnection -> create( reactiveConnection, () -> new ReactiveSessionImpl( delegate, options, reactiveConnection ) ) )
				.map( s -> new MutinySessionImpl(s, this) );
	}

	Uni<Mutiny.Session> newSession(String tenantId) throws HibernateException {
		return uni( () -> connection( tenantId ) )
				.chain( reactiveConnection -> create( reactiveConnection, () -> new ReactiveSessionImpl( delegate, options( tenantId ), reactiveConnection ) ) )
				.map( s -> new MutinySessionImpl(s, this) );
	}

	/**
	 * Close the connection if something goes wrong during the creation of the session
	 */
	private <S> Uni<S> create(ReactiveConnection connection, Supplier<S> supplier) {
		return Uni.createFrom().item( supplier )
				.onFailure().call( () -> Uni.createFrom().completionStage( connection.close() ) );
	}

	@Override
	public Mutiny.StatelessSession openStatelessSession() {
		SessionCreationOptions options = options();
		return new MutinyStatelessSessionImpl(
				new ReactiveStatelessSessionImpl( delegate, options, proxyConnection( options.getTenantIdentifier() ) ),
				this
		);
	}

	public Mutiny.StatelessSession openStatelessSession(String tenantId) {
		return new MutinyStatelessSessionImpl(
				new ReactiveStatelessSessionImpl( delegate, options( tenantId ), proxyConnection( tenantId ) ),
				this
		);
	}

	Uni<Mutiny.StatelessSession> newStatelessSession() throws HibernateException {
		SessionCreationOptions options = options();
		return uni( () -> connection( options.getTenantIdentifier() ) )
				.chain( reactiveConnection -> create( reactiveConnection, () -> new ReactiveStatelessSessionImpl( delegate, options, reactiveConnection ) ) )
				.map( s -> new MutinyStatelessSessionImpl(s, this) );
	}

	Uni<Mutiny.StatelessSession> newStatelessSession(String tenantId) {
		return uni( () -> connection( tenantId ) )
				.chain( reactiveConnection -> create( reactiveConnection, () -> new ReactiveStatelessSessionImpl( delegate, options( tenantId ), reactiveConnection ) ) )
				.map( s -> new MutinyStatelessSessionImpl( s, this ) );
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
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Mutiny.Session current = context.get( Mutiny.Session.class, contextKeyForSession );
		if ( current != null && current.isOpen() ) {
			return work.apply( current );
		}
		return withSession( Mutiny.Session.class, newSession(), work, contextKeyForSession );
	}

	@Override
	public <T> Uni<T> withSession(String tenantId, Function<Mutiny.Session, Uni<T>> work) {
		Objects.requireNonNull( tenantId, "parameter 'tenantId' is required" );
		Objects.requireNonNull( work, "parameter 'work' is required" );
		String id = contextKeyForSession + '/' + tenantId;
		Mutiny.Session current = context.get(Mutiny.Session.class, id);
		if ( current!=null && current.isOpen() ) {
			return work.apply( current );
		}
		return withSession( Mutiny.Session.class, newSession( tenantId ), work, id );
	}

	@Override
	public <T> Uni<T> withStatelessSession(Function<Mutiny.StatelessSession, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		Mutiny.StatelessSession current = context.get( Mutiny.StatelessSession.class, contextKeyForStatelessSession );
		if ( current != null && current.isOpen() ) {
			return work.apply( current );
		}
		return withSession( Mutiny.StatelessSession.class, newStatelessSession(), work, contextKeyForStatelessSession );
	}

	@Override
	public <T> Uni<T> withStatelessSession(String tenantId, Function<Mutiny.StatelessSession, Uni<T>> work) {
		String id = contextKeyForStatelessSession + '/' + tenantId;
		Mutiny.StatelessSession current = context.get( Mutiny.StatelessSession.class, id );
		if ( current != null && current.isOpen() ) {
			return work.apply( current );
		}
		return withSession( Mutiny.StatelessSession.class, newStatelessSession(tenantId), work, id );
	}

	private<S extends Mutiny.Closeable, T> Uni<T> withSession(
			Class<S> sessionType,
			Uni<S> sessionUni,
			Function<S, Uni<T>> work,
			String contextId) {
		return sessionUni.chain( session -> Uni.createFrom().voidItem()
				.invoke( () -> context.put( sessionType, contextId, session ) )
				.chain( () -> work.apply( session ) )
				.eventually( () -> context.remove( sessionType, contextId ) )
				.eventually(session::close)
		);
	}

	@Override
	public <T> Uni<T> withTransaction(BiFunction<Mutiny.Session, Mutiny.Transaction, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
		return withSession( s -> s.withTransaction( t -> work.apply(s, t) ) );
	}

	@Override
	public <T> Uni<T> withTransaction(String tenantId, BiFunction<Mutiny.Session, Mutiny.Transaction, Uni<T>> work) {
		Objects.requireNonNull( work, "parameter 'work' is required" );
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

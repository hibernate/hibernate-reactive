package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.HibernateException;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.impl.ReactiveSessionInternalImpl;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import java.util.function.Function;

/**
 * Implementation of {@link Mutiny.SessionFactory}.
 *
 * @see org.hibernate.reactive.boot.impl.ReactiveSessionFactoryBuilder
 */
public class MutinySessionFactoryImpl implements Mutiny.SessionFactory {

	private SessionFactoryImpl delegate;

	public MutinySessionFactoryImpl(SessionFactoryImpl delegate) {
		this.delegate = delegate;
	}

	@Override
	public Uni<Mutiny.Session> openReactiveSession() throws HibernateException {
		ReactiveConnectionPool pool = delegate.getServiceRegistry()
				.getService(ReactiveConnectionPool.class);
		return Uni.createFrom().completionStage( pool.getConnection() )
				.map( reactiveConnection -> new ReactiveSessionInternalImpl( delegate,
						new SessionFactoryImpl.SessionBuilderImpl<>(delegate), reactiveConnection ) )
				.map( MutinySessionImpl::new );
	}

	@Override
	public <T> Uni<T> withReactiveSession(Function<Mutiny.Session, Uni<T>> work) {
		return openReactiveSession().flatMap(
				session -> work.apply( session ).on().termination( session::close )
		);
	}

	@Override
	public void close() {
		delegate.close();
	}
}

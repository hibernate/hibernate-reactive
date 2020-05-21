package org.hibernate.reactive.stage.impl;

import org.hibernate.HibernateException;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.impl.ReactiveSessionInternalImpl;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.stage.Stage;

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
	public CompletionStage<Stage.Session> openReactiveSession() throws HibernateException {
		ReactiveConnectionPool pool = delegate.getServiceRegistry()
				.getService(ReactiveConnectionPool.class);
		return pool.getConnection()
				.thenApply( reactiveConnection -> new ReactiveSessionInternalImpl( delegate,
						new SessionFactoryImpl.SessionBuilderImpl<>(delegate), reactiveConnection ) )
				.thenApply( StageSessionImpl::new );
	}

	@Override
	public <T> CompletionStage<T> withReactiveSession(Function<Stage.Session, CompletionStage<T>> work) {
		return openReactiveSession().thenCompose(
				session -> work.apply(session)
						.whenComplete( (r, e) -> session.close() )
		);
	}

	@Override
	public void close() {
		delegate.close();
	}
}

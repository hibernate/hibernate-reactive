/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Managed the creation of a {@link SessionFactory} that can shared among tests.
 */
public class SessionFactoryManager {

	private SessionFactory sessionFactory;
	private ReactiveConnectionPool poolProvider;

	public SessionFactoryManager() {
	}

	private boolean needsStart() {
		return sessionFactory == null || sessionFactory.isClosed();
	}

	public void start(Supplier<SessionFactory> supplier) {
		if ( needsStart() ) {
			sessionFactory = supplier.get();
			poolProvider = sessionFactory
					.unwrap( SessionFactoryImplementor.class )
					.getServiceRegistry().getService( ReactiveConnectionPool.class );
		}
	}

	public boolean isStarted() {
		return sessionFactory != null;
	}

	public SessionFactory getHibernateSessionFactory() {
		return sessionFactory;
	}

	public ReactiveConnectionPool getReactiveConnectionPool() {
		return poolProvider;
	}

	public CompletionStage<Void> stop() {
		CompletionStage<Void> releasedStage = voidFuture();
		if ( sessionFactory != null && sessionFactory.isOpen() ) {
			releasedStage = releasedStage
					.whenComplete( (unused, throwable) -> sessionFactory.close() );
		}
		return releasedStage
				.thenCompose( unused -> {
					final CompletionStage<Void> closeFuture;
					if ( poolProvider == null ) {
						closeFuture = voidFuture();
					}
					else {
						closeFuture = poolProvider.getCloseFuture();
					}
					poolProvider = null;
					sessionFactory = null;
					return closeFuture;
				} );
	}
}

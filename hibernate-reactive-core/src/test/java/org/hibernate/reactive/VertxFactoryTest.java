/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.vertx.VertxFactory;
import org.hibernate.reactive.vertx.VertxInstance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.BaseReactiveTest.setDefaultProperties;

@Timeout(value = 10, unit = MINUTES)
public class VertxFactoryTest {

	@Test
	public void createsAndClosesVertxWithFactoryNamedBySetting() {
		CountingVertxFactory.reset();

		SessionFactory sessionFactory = buildSessionFactory( CountingVertxFactory.class.getName() );
		Vertx vertx = vertx( sessionFactory );

		assertThat( CountingVertxFactory.created.get() ).isEqualTo( 1 );
		assertThat( vertx ).isSameAs( CountingVertxFactory.vertx );
		assertThat( isClosed( vertx ) ).isFalse();

		sessionFactory.close();
		assertThat( isClosed( vertx ) ).isTrue();
	}

	// Like the previous test, but the setting is an instance of the factory
	@Test
	public void createsAndClosesVertxWithFactoryInstance() {
		AtomicInteger created = new AtomicInteger();
		Vertx factoryVertx = Vertx.vertx();
		VertxFactory factory = () -> {
			created.incrementAndGet();
			return factoryVertx;
		};

		SessionFactory sessionFactory = buildSessionFactory( factory );

		assertThat( created.get() ).isEqualTo( 1 );
		assertThat( vertx( sessionFactory ) ).isSameAs( factoryVertx );

		sessionFactory.close();
		assertThat( isClosed( factoryVertx ) ).isTrue();
	}

	@Test
	public void usesVertxOfCurrentContextWithoutCallingFactory() {
		AtomicInteger created = new AtomicInteger();
		VertxFactory factory = () -> {
			created.incrementAndGet();
			return Vertx.vertx();
		};
		Vertx applicationVertx = Vertx.vertx();
		try {
			SessionFactory sessionFactory = applicationVertx
					.executeBlocking( () -> buildSessionFactory( factory ) )
					.await();

			assertThat( created.get() ).isEqualTo( 0 );
			assertThat( vertx( sessionFactory ) ).isSameAs( applicationVertx );

			sessionFactory.close();
			assertThat( isClosed( applicationVertx ) ).isFalse();
		}
		finally {
			applicationVertx.close().await();
		}
	}

	// Building the session factory connects to the database with the instance of Vert.x
	private static SessionFactory buildSessionFactory(Object vertxFactory) {
		Configuration configuration = new Configuration();
		setDefaultProperties( configuration );
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() )
				.applySetting( Settings.VERTX_FACTORY, vertxFactory );
		return configuration.buildSessionFactory( builder.build() );
	}

	private static Vertx vertx(SessionFactory sessionFactory) {
		return sessionFactory.unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry().requireService( VertxInstance.class )
				.getVertx();
	}

	private static boolean isClosed(Vertx vertx) {
		return ( (VertxInternal) vertx ).closeFuture().isClosed();
	}

	public static class CountingVertxFactory implements VertxFactory {
		static final AtomicInteger created = new AtomicInteger();
		static volatile Vertx vertx;

		static void reset() {
			created.set( 0 );
			vertx = null;
		}

		@Override
		public Vertx createVertx() {
			created.incrementAndGet();
			vertx = Vertx.vertx();
			return vertx;
		}
	}
}

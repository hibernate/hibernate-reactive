/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.Interceptor;
import org.hibernate.cfg.Configuration;
import org.hibernate.type.Type;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Port of ORM's StatelessSessionInterceptorOnLoadTest (HHH-20876).
 * Verifies that {@link Interceptor#onLoad} is called for stateless session
 * entity loads via get, getMultiple, and refresh.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class StatelessSessionInterceptorOnLoadTest extends BaseReactiveTest {

	private static final OnLoadInterceptor interceptor = new OnLoadInterceptor();

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Document.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setInterceptor( interceptor );
		return configuration;
	}

	@Test
	public void testOnLoadCalledWhenEntityLoaded(VertxTestContext context) {
		interceptor.reset();
		test( context, getSessionFactory()
				.withStatelessTransaction( ss -> ss
						.insert( new Document( "Doc1", "text1" ) )
				)
				.thenCompose( v -> getSessionFactory()
						.withStatelessSession( ss -> {
							interceptor.reset();
							return ss.get( Document.class, "Doc1" )
									.thenAccept( doc -> {
										assertThat( doc ).isNotNull();
										assertThat( interceptor.wasOnLoadCalled() ).isTrue();
									} );
						} )
				)
		);
	}

	@Test
	public void testOnLoadCalledWithRefresh(VertxTestContext context) {
		interceptor.reset();
		test( context, getSessionFactory()
				.withStatelessTransaction( ss -> ss
						.insert( new Document( "Doc2", "text2" ) )
				)
				.thenCompose( v -> getSessionFactory()
						.withStatelessSession( ss -> ss.get( Document.class, "Doc2" )
								.thenCompose( doc -> {
									interceptor.reset();
									return ss.refresh( doc )
											.thenAccept( vv -> assertThat( interceptor.wasOnLoadCalled() ).isTrue() );
								} )
						)
				)
		);
	}

	@Test
	public void testOnLoadCalledWithGetMultiple(VertxTestContext context) {
		interceptor.reset();
		test( context, getSessionFactory()
				.withStatelessTransaction( ss -> ss
						.insert( new Document( "Doc3", "text3" ) )
						.thenCompose( v -> ss.insert( new Document( "Doc4", "text4" ) ) )
						.thenCompose( v -> ss.insert( new Document( "Doc5", "text5" ) ) )
				)
				.thenCompose( v -> getSessionFactory()
						.withStatelessSession( ss -> {
							interceptor.reset();
							return ss.get( Document.class, "Doc3", "Doc4", "Doc5" )
									.thenAccept( docs -> {
										assertThat( docs ).hasSize( 3 );
										assertThat( interceptor.getOnLoadCallCount() ).isEqualTo( 3 );
									} );
						} )
				)
		);
	}

	@Test
	public void testMutinyOnLoadCalledWhenEntityLoaded(VertxTestContext context) {
		interceptor.reset();
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( ss -> ss
						.insert( new Document( "Doc6", "text6" ) )
				)
				.chain( () -> getMutinySessionFactory()
						.withStatelessSession( ss -> {
							interceptor.reset();
							return ss.get( Document.class, "Doc6" )
									.invoke( doc -> {
										assertThat( doc ).isNotNull();
										assertThat( interceptor.wasOnLoadCalled() ).isTrue();
									} );
						} )
				)
		);
	}

	@Test
	public void testMutinyOnLoadCalledWithRefresh(VertxTestContext context) {
		interceptor.reset();
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( ss -> ss
						.insert( new Document( "Doc7", "text7" ) )
				)
				.chain( () -> getMutinySessionFactory()
						.withStatelessSession( ss -> ss.get( Document.class, "Doc7" )
								.chain( doc -> {
									interceptor.reset();
									return ss.refresh( doc )
											.invoke( () -> assertThat( interceptor.wasOnLoadCalled() ).isTrue() );
								} )
						)
				)
		);
	}

	@Test
	public void testMutinyOnLoadCalledWithGetMultiple(VertxTestContext context) {
		interceptor.reset();
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( ss -> ss
						.insert( new Document( "Doc8", "text8" ) )
						.chain( () -> ss.insert( new Document( "Doc9", "text9" ) ) )
						.chain( () -> ss.insert( new Document( "Doc10", "text10" ) ) )
				)
				.chain( () -> getMutinySessionFactory()
						.withStatelessSession( ss -> {
							interceptor.reset();
							return ss.get( Document.class, "Doc8", "Doc9", "Doc10" )
									.invoke( docs -> {
										assertThat( docs ).hasSize( 3 );
										assertThat( interceptor.getOnLoadCallCount() ).isEqualTo( 3 );
									} );
						} )
				)
		);
	}

	public static class OnLoadInterceptor implements Interceptor {
		private final AtomicBoolean onLoadCalled = new AtomicBoolean( false );
		private final AtomicInteger onLoadCallCount = new AtomicInteger( 0 );

		@Override
		public boolean onLoad(Object entity, Object id, Object[] state, String[] propertyNames, Type[] types) {
			onLoadCalled.set( true );
			onLoadCallCount.incrementAndGet();
			return false;
		}

		public boolean wasOnLoadCalled() {
			return onLoadCalled.get();
		}

		public int getOnLoadCallCount() {
			return onLoadCallCount.get();
		}

		public void reset() {
			onLoadCalled.set( false );
			onLoadCallCount.set( 0 );
		}
	}

	@Entity(name = "Document")
	@Table(name = "Document")
	public static class Document {
		@Id
		public String name;
		public String text;
		@Version
		public int version;

		Document() {
		}

		Document(String name, String text) {
			this.name = name;
			this.text = text;
		}
	}
}

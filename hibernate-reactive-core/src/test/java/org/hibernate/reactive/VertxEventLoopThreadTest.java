/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Context;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Check that the {@link BaseReactiveTest} class is starting a Vert.x event loop
 * and that a test will use it.
 * <p>
 *    This is important because our assumption is that all tests will run in a single thread and it's
 *    the Vert.x event loop one.
 * </p>
 * <p>
 *    If the test is simple enough, it might not be important but with complex tests,
 *    breaking the single thread assumption, could lead to problems if the context is
 *    not shared properly among threads.
 * </p>
 */
public class VertxEventLoopThreadTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Boardgame.class );
	}

	@Test
	public void testThreadsWithCompletionStage(VertxTestContext context) {
		final Thread currentThread = Thread.currentThread();
		assertThat( Context.isOnEventLoopThread() )
				.as( "This is not a Vert.x event loop thread " + currentThread )
				.isTrue();

		test( context, openSession().thenCompose( session -> session
				.find( Boardgame.class, "Wingspan" )
				.thenAccept( v -> {
					Thread insideThread = Thread.currentThread();
					assertEquals( currentThread, insideThread );
				} ) ) );
	}

	@Test
	public void testThreadsWithMutiny(VertxTestContext context) {
		Thread currentThread = Thread.currentThread();
		assertThat( Context.isOnEventLoopThread() )
				.as( "This is not a Vert.x event loop thread " + currentThread )
				.isTrue();

		test( context, openMutinySession().chain( session -> session
				.find( Boardgame.class, "The Crew: the quest for planet nine" )
				.invoke( v -> {
					Thread insideThread = Thread.currentThread();
					assertEquals( currentThread, insideThread );
				} ) ) );
	}

	@Entity(name = "Boardgame")
	@Table(name = "Boardgame")
	static class Boardgame {
		@Id
		String name;

		public Boardgame() {
		}

		public Boardgame(String name) {
			this.name = name;
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.core.Context;
import io.vertx.ext.unit.TestContext;
import static org.assertj.core.api.Assertions.assertThat;

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
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Boardgame.class );
		return configuration;
	}

	@Test
	public void testThreadsWithCompletionStage(TestContext context) {
		final Thread currentThread = Thread.currentThread();
		assertThat( Context.isOnEventLoopThread() )
				.as( "This is not a Vert.x event loop thread " + currentThread )
				.isTrue();

		test( context, openSession()
			.find( Boardgame.class, "Wingspan" )
			.thenAccept( v -> {
				Thread insideThread = Thread.currentThread();
				context.assertEquals( currentThread, insideThread );
			} ) );
	}

	@Test
	public void testThreadsWithMutiny(TestContext context) {
		Thread currentThread = Thread.currentThread();
		assertThat( Context.isOnEventLoopThread() )
				.as( "This is not a Vert.x event loop thread " + currentThread )
				.isTrue();

		test( context, openMutinySession()
				.find( Boardgame.class, "The Crew: the quest for planet nine" )
				.invoke( v -> {
					Thread insideThread = Thread.currentThread();
					context.assertEquals( currentThread, insideThread );
				} ) );
	}

	@Entity
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

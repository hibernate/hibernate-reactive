/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

/**
 * Checks that the connection can be used from any Vert.x context that shares the root context
 * (or the event loop) of the context where the session was opened.
 * <p>
 * Frameworks like Quarkus switch to a different duplicated context when they propagate the context
 * to a task or when they run a REST client invocation, and that's safe.
 * Using the session from another event loop is an error instead.
 * </p>
 *
 * @see org.hibernate.reactive.common.InternalStateAssertions
 * @see InternalStateAssertionsTest
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class InternalStateAssertionsContextTest extends BaseReactiveTest {

	/**
	 * @see org.hibernate.reactive.logging.internal.Log#detectedUsedOfTheSessionOnTheWrongThread
	 */
	private static final String WRONG_THREAD_ERROR_CODE = "HR000069";

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Gadget.class );
	}

	@Test
	public void testSameContext(VertxTestContext testContext) {
		ContextInternal creator = duplicateOfTestContext();
		test( testContext, persistFromContext( creator, creator ) );
	}

	@Test
	public void testDuplicateOfTheCreatorContext(VertxTestContext testContext) {
		ContextInternal creator = duplicateOfTestContext();
		test( testContext, persistFromContext( creator, creator.duplicate() ) );
	}

	@Test
	public void testSiblingDuplicateContext(VertxTestContext testContext) {
		test( testContext, persistFromContext( duplicateOfTestContext(), duplicateOfTestContext() ) );
	}

	@Test
	public void testRootContext(VertxTestContext testContext) {
		test( testContext, persistFromContext( duplicateOfTestContext(), testContext() ) );
	}

	@Test
	public void testContextOnAnotherEventLoop(VertxTestContext testContext) {
		ContextInternal creator = duplicateOfTestContext();
		test( testContext, assertThrown( IllegalStateException.class, persistFromContext( creator, duplicateOnAnotherEventLoop( creator ) ) )
				.invoke( e -> assertThat( e.getMessage() ).startsWith( WRONG_THREAD_ERROR_CODE ) ) );
	}

	@Test
	public void testSessionFromTransactionUsedOnDuplicateWithCopiedLocals(VertxTestContext testContext) {
		ContextInternal creator = duplicateOfTestContext();
		Gadget first = new Gadget( "first" );
		Gadget second = new Gadget( "second" );

		test( testContext, Uni.createFrom().voidItem().emitOn( on( creator ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.persist( first )
						.call( session::flush )
						// Same as Quarkus context propagation: a new duplicate with a copy of the context locals
						.chain( () -> Uni.createFrom().voidItem().emitOn( on( duplicateWithCopiedLocals( creator ) ) ) )
						.chain( () -> getMutinySessionFactory().withSession( current -> {
							assertThat( current ).isSameAs( session );
							return current.persist( second ).call( current::flush );
						} ) ) ) )
				.chain( () -> Uni.createFrom().voidItem().emitOn( on( testContext() ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.createSelectionQuery( "from Gadget", Gadget.class )
						.getResultList() ) )
				.invoke( gadgets -> assertThat( gadgets ).containsExactlyInAnyOrder( first, second ) )
		);
	}

	/**
	 * Open a session in the {@code creator} context and use it in the {@code user} one.
	 */
	private Uni<Void> persistFromContext(ContextInternal creator, ContextInternal user) {
		Gadget gadget = new Gadget( "gadget" );
		return Uni.createFrom().voidItem().emitOn( on( creator ) )
				.chain( () -> getMutinySessionFactory().openSession() )
				.chain( session -> Uni.createFrom().voidItem().emitOn( on( user ) )
						.chain( () -> session.persist( gadget ) )
						.chain( session::flush )
						.invoke( session::clear )
						.chain( () -> session.find( Gadget.class, gadget.id ) )
						.invoke( found -> assertThat( found ).isEqualTo( gadget ) )
						// Sessions must be closed in the context where they were opened
						.eventually( () -> Uni.createFrom().voidItem().emitOn( on( creator ) ).chain( session::close ) ) )
				.replaceWithVoid();
	}

	private static Executor on(ContextInternal context) {
		return command -> context.runOnContext( v -> command.run() );
	}

	/**
	 * The context the test is running in (a root context).
	 */
	private static ContextInternal testContext() {
		return (ContextInternal) Vertx.currentContext();
	}

	private static ContextInternal duplicateOfTestContext() {
		return testContext().duplicate();
	}

	private ContextInternal duplicateOnAnotherEventLoop(ContextInternal context) {
		VertxInternal vertx = (VertxInternal) testOnContext.vertx();
		ContextInternal other;
		do {
			other = vertx.createEventLoopContext();
		}
		while ( other.nettyEventLoop() == context.nettyEventLoop() );
		return other.duplicate();
	}

	/**
	 * Hibernate Reactive stores the session in a context local: copy it to a new duplicate
	 * like Quarkus does when it propagates the context.
	 */
	private static ContextInternal duplicateWithCopiedLocals(ContextInternal context) {
		ContextInternal duplicate = context.duplicate();
		duplicate.localContextData().putAll( context.localContextData() );
		return duplicate;
	}

	@Entity(name = "Gadget")
	static class Gadget {
		@Id
		@GeneratedValue
		Long id;

		String name;

		public Gadget() {
		}

		public Gadget(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Gadget gadget = (Gadget) o;
			return Objects.equals( name, gadget.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}
}

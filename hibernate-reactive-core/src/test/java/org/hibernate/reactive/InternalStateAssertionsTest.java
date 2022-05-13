/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.assertj.core.api.Assertions;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.runOnlyFor;

/**
 * It's currently considered an error to share a Session between multiple reactive streams,
 * so we should detect that condition and throw an exception.
 * <p>
 * WARNING: Because we are running the code to test inside a function, we must create the {@link Async}
 * in advance. Otherwise the test will end successfully because the async has not been created yet.
 * </p>
 */
public class InternalStateAssertionsTest extends BaseReactiveTest {

	private static final String ERROR_MESSAGE_LOWER_CASED = "Detected use of the reactive Session from a different Thread"
			.toLowerCase( Locale.ROOT );

	private Object currentSession;

	// These tests will fail before touching the database, so there is no reason
	// to run them on all databases
	@Rule
	public DatabaseSelectionRule rule = runOnlyFor( POSTGRESQL );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Competition.class );
	}

	@After
	public void closeEverything(TestContext context) {
		test( context, closeSession( currentSession ) );
	}

	@Test
	@Ignore
	// I don't know why but this test fails on CI because no exception is thrown
	public void testPersistWithStage(TestContext testContext) {
		Async async = testContext.async();
		CompletionStage<Stage.Session> sessionStage = getSessionFactory().openSession();
		currentSession = sessionStage;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		test( testContext, sessionStage.thenAccept( session ->
			// Run test in the new context
			newContext.runOnContext( event -> test( async, testContext, session
					.persist( new Competition( "Cheese Rolling" ) )
					.handle( (v, e) -> assertExceptionThrown( e ).join() ) )
			)
		) );
	}

	@Test
	public void testFindWithStage(TestContext testContext) {
		Async async = testContext.async();
		CompletionStage<Stage.Session> sessionStage = getSessionFactory().openSession();
		currentSession = sessionStage;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		test( testContext, sessionStage.thenAccept( session ->
			 // Run test in the new context
			 newContext.runOnContext( event -> test( async, testContext, session
					 .find( Competition.class, "Chess boxing" )
					 .handle( (v, e) -> assertExceptionThrown( e ).join() ) )
			 )
	  	) );
	}

	@Test
	public void testOnPersistWithMutiny(TestContext testContext) {
		final Async async = testContext.async();
		Uni<Mutiny.Session> sessionUni = getMutinySessionFactory().openSession();
		currentSession = sessionUni;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		test( testContext, sessionUni.invoke( session ->
			// Run test in the new context
			newContext.runOnContext( event -> test( async, testContext, session
					.persist( new Competition( "Cheese Rolling" ) )
					.onItemOrFailure()
					.transformToUni( (unused, e) -> Uni.createFrom().completionStage( assertExceptionThrown( e ) ) ) )
			)
		) );
	}

	@Test
	public void testFindWithMutiny(TestContext testContext) {
		final Async async = testContext.async();
		Uni<Mutiny.Session> sessionUni = getMutinySessionFactory().openSession();
		currentSession = sessionUni;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		test( testContext, sessionUni.invoke( session -> {
			// Run test in the new context
			newContext.runOnContext( event -> test( async, testContext, session
					.find( Competition.class, "Chess boxing" )
					.onItemOrFailure()
					.transformToUni( (unused, e) -> Uni.createFrom().completionStage( assertExceptionThrown( e ) ) ) )
			);
		} ) );
	}

	// Check that at least one exception has the expected message
	private static CompletableFuture<Void> assertExceptionThrown(Throwable e) {
		CompletableFuture<Void> result = new CompletableFuture<>();
		Throwable t = e;
		while ( t != null ) {
			if ( t.getClass().equals( IllegalStateException.class )
					&& expectedMessage( t ) ) {
				result.complete( null );
				return result;
			}
			t = t.getCause();
		}
		result.completeExceptionally( new AssertionError( "Expected exception not thrown. Exception thrown: " + e ) );
		return result;
	}

	private static boolean expectedMessage(Throwable t) {
		return t.getMessage().toLowerCase( Locale.ROOT )
				.contains( ERROR_MESSAGE_LOWER_CASED );
	}

	@Entity
	static class Competition {
		@Id
		String name;

		public Competition() {
		}

		public Competition(String name) {
			this.name = name;
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.assertj.core.api.Assertions;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.runOnlyFor;

/**
 * It's currently considered an error to share a Session between multiple reactive streams,
 * so we should detect that condition and throw an exception.
 */
public class MultipleContextTest extends BaseReactiveTest {

	private static final String ERROR_MESSAGE_LOWER_CASED = "HR000069: Detected use of the reactive Session from a different Thread"
			.toLowerCase( Locale.ROOT );

	private Object currentSession;

	// These tests will fail before touching the database, so there is no reason
	// to run them on all databases
	@Rule
	public DatabaseSelectionRule rule = runOnlyFor( POSTGRESQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Competition.class );
		return configuration;
	}

	@After
	public void closeEverything(TestContext context) {
		test( context, closeSession( currentSession ) );
	}

	@Test
	public void testPersistWithStage(TestContext testContext) {
		CompletionStage<Stage.Session> sessionStage = getSessionFactory().openSession();
		currentSession = sessionStage;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		CompletableFuture<Throwable> thrown = new CompletableFuture<>();

		sessionStage.thenAccept( session -> newContext
				// Execute persist in the new context
				.runOnContext( event -> session
						.persist( new Competition( "Cheese Rolling" ) )
						.handle( this::catchException )
						.thenAccept( thrown::complete )
				)
		);

		test( testContext, thrown.thenCompose( MultipleContextTest::assertExceptionThrown ) );
	}

	@Test
	public void testFindWithStage(TestContext testContext) {
		CompletionStage<Stage.Session> sessionStage = getSessionFactory().openSession();
		currentSession = sessionStage;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		CompletableFuture<Throwable> thrown = new CompletableFuture<>();
		sessionStage.thenAccept( session -> newContext
				// Execute find in the new context
				.runOnContext( event -> session
						.find( Competition.class, "Chess boxing" )
						.handle( this::catchException )
						.thenAccept( thrown::complete )
				)
		);

		test( testContext, thrown.thenCompose( MultipleContextTest::assertExceptionThrown ) );
	}

	/**
	 * Keep track of the exception thrown (if any) and continue without failures
	 */
	private Throwable catchException(Object ignore, Throwable throwable) {
		return throwable;
	}

	@Test
	public void testOnPersistWithMutiny(TestContext testContext) {
		Uni<Mutiny.Session> sessionUni = getMutinySessionFactory().openSession();
		currentSession = sessionUni;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		test( testContext, sessionUni
				.chain( session -> {
					CompletableFuture<Object> thrown = new CompletableFuture<>();
					// Execute persist in the new context
					newContext.runOnContext( event -> session
							.persist( new Competition( "Cheese Rolling" ) )
							.subscribe()
							.with( thrown::complete, thrown::complete )
					);
					return Uni.createFrom().completionStage( thrown );
				} )
				.chain( MultipleContextTest::assertExceptionThrownAsUni )
		);
	}

	@Test
	public void testFindWithMutiny(TestContext testContext) {
		Uni<Mutiny.Session> sessionUni = getMutinySessionFactory().openSession();
		currentSession = sessionUni;
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Context newContext = Vertx.vertx().getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		test( testContext, sessionUni
				.chain( session -> {
					CompletableFuture<Object> thrown = new CompletableFuture<>();
					// Execute find in the new context
					newContext.runOnContext( event -> session
							.find( Competition.class, "Chess boxing" )
							.subscribe()
							.with( thrown::complete, thrown::complete )
					);
					return Uni.createFrom().completionStage( thrown );
				} )
				.chain( MultipleContextTest::assertExceptionThrownAsUni )
		);
	}

	private static Uni<? extends Void> assertExceptionThrownAsUni(Object e) {
		return Uni.createFrom().completionStage( assertExceptionThrown( (Throwable) e ) );
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

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.CompletionException;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.assertj.core.api.Assertions;

/**
 * It's currently considered an error to share a Session between multiple reactive streams,
 * so we should detect that condition and throw an exception.
 */
public class MultipleContextTest extends BaseReactiveTest {

	private static final String ERROR_MESSAGE = "Detected use of the reactive Session from a different Thread";

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Competition.class );
		return configuration;
	}

	@Test
	public void testPersistWithStage(TestContext testContext) throws Exception {
		Stage.Session session = openSession();
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Vertx vertx = Vertx.vertx();
		Context newContext = vertx.getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		// Run test in the new context
		newContext.runOnContext( event ->
			test( testContext, session
					.persist( new Competition( "Cheese Rolling" ) )
					.handle( (v, e) -> {
						testContext.assertNotNull( e );
						testContext.assertEquals( CompletionException.class, e.getClass() );
						testContext.assertEquals( IllegalStateException.class, e.getCause().getClass() );
						testContext.assertTrue( e.getMessage().contains( ERROR_MESSAGE ) );
						return null;
					} ) )
		);
	}


	@Test
	public void testFindWithStage(TestContext testContext) throws Exception {
		Stage.Session session = openSession();
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Vertx vertx = Vertx.vertx();
		Context newContext = vertx.getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		// Run test in the new context
		newContext.runOnContext( event ->
			 test( testContext, session
					 .find( Competition.class, "Chess boxing" )
					 .handle( (v, e) -> {
						 testContext.assertNotNull( e );
						 testContext.assertEquals( CompletionException.class, e.getClass() );
						 testContext.assertEquals( IllegalStateException.class, e.getCause().getClass() );
						 testContext.assertTrue( e.getMessage().contains( ERROR_MESSAGE ) );
						 return null;
					 } ) )
		);
	}

	@Test
	public void testOnPersistWithMutiny(TestContext testContext) throws Exception {
		Mutiny.Session session = openMutinySession();
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Vertx vertx = Vertx.vertx();
		Context newContext = vertx.getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		// Run test in the new context
		newContext.runOnContext( event ->
			 test( testContext, session
					 .persist( new Competition( "Cheese Rolling" ) )
					 .onItem().invoke( v -> testContext.fail( "We were expecting an exception" ) )
					 .onFailure().recoverWithUni( e -> {
						 testContext.assertEquals( IllegalStateException.class, e.getClass() );
						 testContext.assertTrue( e.getMessage().contains( ERROR_MESSAGE ) );
						 return Uni.createFrom().voidItem();
					 } ) )
		);
	}

	@Test
	public void testFindWithMutiny(TestContext testContext) throws Exception {
		Mutiny.Session session = openMutinySession();
		Context testVertxContext = Vertx.currentContext();

		// Create a different new context
		Vertx vertx = Vertx.vertx();
		Context newContext = vertx.getOrCreateContext();
		Assertions.assertThat( testVertxContext ).isNotEqualTo( newContext );

		// Run test in the new context
		newContext.runOnContext(event ->
			 test( testContext, session
					 .find( Competition.class, "Chess boxing" )
					 .onItem().invoke( v -> testContext.fail( "We were expecting an exception" ) )
					 .onFailure().recoverWithUni( e -> {
						 testContext.assertEquals( IllegalStateException.class, e.getClass() );
						 testContext.assertTrue( e.getMessage().contains( ERROR_MESSAGE ) );
						 return Uni.createFrom().nullItem();
					 } ) )
		);
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

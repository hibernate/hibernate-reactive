/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;
import org.hibernate.reactive.pool.impl.ResourceLocalTransactionCoordinator;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class NoLiveTransactionValidationErrorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Comic.class );
	}

	/**
	 * @see ExternalTransactionCoordinatorTest for when the transaction lifecycle is externally managed
	 */
	@Test
	public void defaultConnectionUsesResourceLocalCoordinator(VertxTestContext context) {
		test( context, getSessionFactory()
				.withSession( session -> {
					ReactiveConnection connection = ( (StageSessionImpl) session ).getReactiveConnection();

					ReactiveTransactionCoordinator coordinator = connection.getTransactionCoordinator();
					assertThat( coordinator )
							.as( "Default connection should use ResourceLocalTransactionCoordinator" )
							.isSameAs( ResourceLocalTransactionCoordinator.INSTANCE );

					assertThat( coordinator.isExternallyManaged() )
							.as( "Default transactions should not be externally managed" )
							.isFalse();

					return voidFuture();
				} )
		);
	}

	@Test
	public void beginTransactionError(VertxTestContext context) {
		final Stage.Session[] session = { null };
		test( context, assertThrown( IllegalStateException.class, getSessionFactory()
				.openSession()
				.thenCompose( s -> {
					session[0] = s;
					ReactiveConnection connection = reactiveConnection( s );
					return connection.beginTransaction()
							.thenCompose( t1 -> connection.beginTransaction() );
				} )
			  )
				.thenAccept( e -> {
					assertThat( e )
							.hasMessageContaining( "HR000091" )
							.hasMessageContaining( "Can't begin a new transaction" );
				} )
				.handle( CompletionStages::handle )
				// We try to close the session we opened
				.thenCompose( assertionHandler -> close( session[0] )
						.thenCompose( assertionHandler::getResultAsCompletionStage ) )
		);
	}

	// Try to close the session ignoring any error
	private static CompletionStage<Void> close( Stage.Session session) {
		return closeSession( session ).handle( (unused, throwable) -> null );
	}

	private static ReactiveConnection reactiveConnection(Stage.Session s) {
		return ( (StageSessionImpl) s ).getReactiveConnection();
	}

	@Test
	public void rollbackOnCloseWithStage(VertxTestContext context) {
		Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );

		test(
				context,
				assertThrown( IllegalStateException.class, getSessionFactory()
						.withTransaction( s -> s
								.persist( beneath )
								.thenCompose( v -> s.flush() )
								// Close the connection before committing
								.thenCompose( v -> s.close() )
						)
				)
						.thenAccept( e -> assertThat( e )
								.hasMessageContaining( "HR000090" )
								.hasMessageContaining( "closing the connection" )
						)
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.thenAccept( result -> assertThat( result )
								.as( "The persist should have been roll backed" )
								.isNull() )
		);
	}

	@Test
	public void rollbackOnErrorWithStage(VertxTestContext context) {
		Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );
		final RuntimeException ohNo = new RuntimeException( "Oh, no!" );
		test(
				context,
				assertThrown( RuntimeException.class, getSessionFactory()
						.withTransaction( s -> s
								.persist( beneath )
								.thenCompose( v -> s.flush() )
								// Close the connection before committing
								.thenAccept( v -> {
									throw ohNo;
								} )
						)
				)
						.thenAccept( e -> assertThat( e ).hasMessageContaining( ohNo.getMessage() ) )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.thenAccept( result -> assertThat( result )
								.as( "The persist should have been roll backed" )
								.isNull() )
		);
	}

	@Test
	public void rollbackOnClose(VertxTestContext context) {
		Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );

		test(
				context,
				assertThrown( IllegalStateException.class, getMutinySessionFactory()
						.withTransaction( s -> s
								.persist( beneath )
								.call( s::flush )
								// Close the connection before committing
								.call( s::close )
						)
				)
						.invoke( e -> assertThat( e )
								.hasMessageContaining( "HR000090" )
								.hasMessageContaining( "closing the connection" )
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.invoke( result -> assertThat( result )
								.as( "The persist should have been roll backed" )
								.isNull() )
		);
	}

	@Test
	public void rollbackOnError(VertxTestContext context) {
		Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );
		final RuntimeException ohNo = new RuntimeException( "Oh, no!" );
		test(
				context,
				assertThrown( RuntimeException.class, getMutinySessionFactory()
						.withTransaction( s -> s
								.persist( beneath )
								.call( s::flush )
								.chain( () -> {
									throw ohNo;
								} )
						)
				)
						.invoke( e -> assertThat( e ).hasMessageContaining( ohNo.getMessage() ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.invoke( result -> assertThat( result )
								.as( "The persist should have been roll backed" )
								.isNull() )
		);
	}

	@Entity
	public static class Comic {
		@Id
		public String isbn;
		public String title;

		public Comic() {
		}

		public Comic(String iban, String title) {
			this.isbn = iban;
			this.title = title;
		}

		@Override
		public String toString() {
			return isbn + ":" + title;
		}
	}
}

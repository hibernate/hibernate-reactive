/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;
import org.hibernate.reactive.pool.impl.ResourceLocalTransactionCoordinator;
import org.hibernate.reactive.stage.impl.StageSessionImpl;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Tests for transaction coordination abstraction when using the resource-local (default)
 * transaction coordinator.
 *
 * @see ExternalTransactionCoordinatorTest
 */
public class ResourceLocalTransactionCoordinatorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Comic.class );
	}

	@Test
	public void resourceLocalCoordinatorIsNotExternallyManaged() {
		assertThat( ResourceLocalTransactionCoordinator.INSTANCE.isExternallyManaged() )
				.as( "ResourceLocalTransactionCoordinator should not be externally managed" )
				.isFalse();

	}

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
	public void closeWithTransactionThrowsErrorForResourceLocal(VertxTestContext context) {
		Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );

		test(
				context,
				getSessionFactory()
						.withSession( session -> {
							ReactiveConnection connection = ( (StageSessionImpl) session ).getReactiveConnection();

							assertThat( connection.getTransactionCoordinator().isExternallyManaged() )
									.as( "Should use resource-local coordinator" )
									.isFalse();

							return assertThrown( IllegalStateException.class, connection.beginTransaction()
									.thenCompose( v -> session.persist( beneath ) )
									.thenCompose( v -> session.flush() )
									.thenCompose( v -> connection.close() ) )
									.thenAccept( error -> assertThat( error )
											.as( "Closing with active transaction should throw error for resource-local" )
											.isNotNull()
											.hasMessageContaining( "HR000090" )
											.hasMessageContaining( "closing the connection" )
									);
						} )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.thenAccept( result -> assertThat( result )
								.as( "The persist operation should have been rolled back" )
								.isNull()
						)
		);
	}

	@Entity
	public static class Comic {
		@Id
		public String isbn;
		public String title;

		public Comic() {
		}

		public Comic(String isbn, String title) {
			this.isbn = isbn;
			this.title = title;
		}

		@Override
		public String toString() {
			return isbn + ":" + title;
		}
	}
}

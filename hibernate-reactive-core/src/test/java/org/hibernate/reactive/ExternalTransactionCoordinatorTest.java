/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.ExternalTransactionCoordinator;
import org.hibernate.reactive.pool.impl.ExternallyManagedConnection;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for transaction coordination abstraction when using an externally-managed transaction
 * coordination to ensure frameworks like Quarkus can properly integrate.
 *
 * @see NoLiveTransactionValidationErrorTest
 */
public class ExternalTransactionCoordinatorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Comic.class );
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		builder.addInitiator( new TestExternalConnectionPoolInitiator() );
	}

	@Test
	public void validationOnSessionClose(VertxTestContext context) {
		final Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );
		TestContextualConnectionPool pool = (TestContextualConnectionPool) factoryManager.getReactiveConnectionPool();

		test(
				context,
				// External transaction manager gets connection and begins transaction
				pool.getConnection()
						.thenCompose( connection -> {
							ExternallyManagedConnection externalConnection = (ExternallyManagedConnection) connection;
							return externalConnection.beginTransaction()
									.thenCompose( v -> {
										assertThat( externalConnection.getTransactionCoordinator().isExternallyManaged() )
												.as( "Should use externally-managed coordinator" )
												.isTrue();

										return getMutinySessionFactory()
												.openSession()
												.chain( session -> {
													ReactiveConnection sessionConnection = ( (MutinySessionImpl) session ).getReactiveConnection();
													assertThat( sessionConnection )
															.as( "Session should use the same externally-managed connection" )
															.isSameAs( externalConnection );

													return doSomethingInTransaction( session, beneath );
												} )
												.subscribeAsCompletionStage();
									} )
									// External manager commits (triggers before-commit actions like flush)
									.thenCompose( vv -> externalConnection.commitTransaction() )
									// External manager closes the underlying connection and clears context
									.thenCompose( vv -> pool.closeConnection() );
						} )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.thenAccept( result -> assertThat( result )
								.as( "The entity should have been persisted in the database" )
								.isEqualTo( beneath )
						)
		);
	}

	@Test
	public void validationOnProxySessionClose(VertxTestContext context) {
		final Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );
		TestContextualConnectionPool pool = (TestContextualConnectionPool) factoryManager.getReactiveConnectionPool();
		// External transaction manager gets connection and begins transaction
		ExternallyManagedConnection externalConnection = (ExternallyManagedConnection) pool.getProxyConnection();
		test(
				context,
				externalConnection
						.beginTransaction()
						.thenCompose( v -> {
							assertThat( externalConnection.getTransactionCoordinator().isExternallyManaged() )
									.as( "Should use externally-managed coordinator" )
									.isTrue();

							Mutiny.Session session = getMutinySessionFactory().createSession();
							ReactiveConnection sessionConnection = ( (MutinySessionImpl) session ).getReactiveConnection();
							assertThat( sessionConnection )
									.as( "Session should use the same externally-managed connection" )
									.isSameAs( externalConnection );

							return doSomethingInTransaction( session, beneath )
									.subscribeAsCompletionStage();
						} )
						// External manager commits (triggers before-commit actions like flush)
						.thenCompose( vv -> externalConnection.commitTransaction() )
						// External manager closes the underlying connection and clears context
						.thenCompose( vv -> pool.closeConnection() )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.thenAccept( result -> assertThat( result )
								.as( "The entity should have been persisted in the database" )
								.isEqualTo( beneath )
						)
		);
	}

	/**
	 * In Quarkus, a user would define the transaction boundaries using annotations ({@code @Transactional} or similar).
	 * The method itself is unaware of the transaction boundaries: it just inject a session via CDI and use it.
	 */
	private Uni<Void> doSomethingInTransaction(Mutiny.Session session, Comic beneath) {
		return session
				.persist( beneath )
				// Having a transaction active shouldn't cause any validation error
				// when we close the session. It will actually do nothing until the transaction completes.
				.call( session::close );
	}

	/**
	 * Service initiator for the test connection pool.
	 */
	static class TestExternalConnectionPoolInitiator implements org.hibernate.boot.registry.StandardServiceInitiator<ReactiveConnectionPool> {
		@Override
		public ReactiveConnectionPool initiateService(Map configurationValues, org.hibernate.service.spi.ServiceRegistryImplementor registry) {
			return new TestContextualConnectionPool();
		}

		@Override
		public Class<ReactiveConnectionPool> getServiceInitiated() {
			return ReactiveConnectionPool.class;
		}
	}

	/**
	 * A pool that returns an {@link ExternalTransactionCoordinator}, just for testing.
	 */
	static class TestContextualConnectionPool extends DefaultSqlClientPool {

		private ExternallyManagedConnection currentConnection;
		private final ExternalTransactionCoordinator coordinator;

		TestContextualConnectionPool() {
			this.coordinator = new ExternalTransactionCoordinator( this::getCurrentConnection );
		}

		@Override
		public ReactiveTransactionCoordinator getTransactionCoordinator() {
			return coordinator;
		}

		private CompletionStage<ReactiveConnection> getCurrentConnection() {
			return currentConnection != null ? completedFuture( currentConnection ) : null;
		}

		CompletionStage<Void> closeConnection() {
			ExternallyManagedConnection connection = currentConnection;
			currentConnection = null;
			return connection != null ? connection.closeUnderlyingConnection() : voidFuture();
		}

		@Override
		public CompletionStage<Void> getCloseFuture() {
			return closeConnection()
					.thenCompose( v -> super.getCloseFuture() );
		}

		@Override
		public CompletionStage<ReactiveConnection> getConnection() {
			if ( currentConnection != null ) {
				return completedFuture( currentConnection );
			}
			return super.getConnection()
					.thenApply( connection -> {
						currentConnection = ExternallyManagedConnection.wrap( connection, coordinator );
						return currentConnection;
					} );
		}

		@Override
		public ReactiveConnection getProxyConnection() {
			if ( currentConnection != null ) {
				return currentConnection;
			}
			ReactiveConnection proxyConnection = super.getProxyConnection();
			currentConnection = ExternallyManagedConnection.wrap( proxyConnection, coordinator );
			return currentConnection;
		}
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

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Comic comic = (Comic) o;
			return Objects.equals( isbn, comic.isbn ) && Objects.equals( title, comic.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( isbn, title );
		}
	}
}

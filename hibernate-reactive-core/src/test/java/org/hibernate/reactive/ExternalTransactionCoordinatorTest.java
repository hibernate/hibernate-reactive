/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for transaction coordination abstraction when using an externally-managed transaction
 * coordination to ensure frameworks like Quarkus can properly integrate.
 *
 * @see ResourceLocalTransactionCoordinatorTest
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
		Comic beneath = new Comic( "979-8887241081", "Beneath The Trees Where Nobody Sees" );

		test(
				context,
				// Get connection directly from the pool (simulating external transaction manager)
				factoryManager.getReactiveConnectionPool()
						.getConnection()
						.thenCompose( connection -> connection
								// External manager begins transaction
								.beginTransaction()
								// Open session using this externally-managed connection
								.thenCompose( v -> getMutinySessionFactory()
										.openSession()
										.chain( session -> session
												.persist( beneath )
												.call( session::flush )
												// Close session (should NOT throw exception, should NOT rollback)
												.call( session::close )
										).subscribeAsCompletionStage()
								)
								// External manager commits after session is closed
								.thenCompose( v -> connection.commitTransaction() )
								// External manager closes connection
								.thenCompose( v -> connection.close() )
						)
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Comic.class, beneath.isbn ) )
						)
						.thenAccept( result -> assertThat( result )
								.as( "The data should have been persisted with externally-managed coordinator" )
								.isNotNull()
								.satisfies( comic -> {
									assertThat( comic.isbn ).isEqualTo( beneath.isbn );
									assertThat( comic.title ).isEqualTo( beneath.title );
								} )
						)
		);
	}

	/**
	 * Service initiator for the test connection pool.
	 */
	static class TestExternalConnectionPoolInitiator implements org.hibernate.boot.registry.StandardServiceInitiator<ReactiveConnectionPool> {
		@Override
		public ReactiveConnectionPool initiateService(Map configurationValues, org.hibernate.service.spi.ServiceRegistryImplementor registry) {
			return new TestExternalConnectionPool();
		}

		@Override
		public Class<ReactiveConnectionPool> getServiceInitiated() {
			return ReactiveConnectionPool.class;
		}
	}

	/**
	 * Test implementation of an externally-managed transaction coordinator.
	 * This simulates what frameworks like Quarkus would implement.
	 */
	private static class TestExternalTransactionCoordinator implements ReactiveTransactionCoordinator {
		public static final TestExternalTransactionCoordinator INSTANCE = new TestExternalTransactionCoordinator();

		private TestExternalTransactionCoordinator() {
		}

		@Override
		public boolean isExternallyManaged() {
			return true;
		}
	}

	/**
	 * Test connection pool that uses the external transaction coordinator.
	 * Extends DefaultSqlClientPool and only overrides the transaction coordinator.
	 */
	static class TestExternalConnectionPool extends DefaultSqlClientPool {
		@Override
		protected ReactiveTransactionCoordinator getTransactionCoordinator() {
			return TestExternalTransactionCoordinator.INSTANCE;
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
	}
}

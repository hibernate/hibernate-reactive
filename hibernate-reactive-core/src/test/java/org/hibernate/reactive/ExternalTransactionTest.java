/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.impl.ExternalTransaction;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionImpl;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@code currentTransaction()} and {@code withTransaction()} correctly
 * detect and join transactions that were opened externally on the underlying
 * connection (e.g., by a framework-level connection pool), rather than through
 * the session's own {@code withTransaction()} method.
 *
 * @see <a href="https://github.com/hibernate/hibernate-reactive/issues/2852">#2852</a>
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class ExternalTransactionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Thing.class );
	}

	@Test
	public void testMutinyCurrentTransactionDetectsExternalTransaction(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.openSession()
						.chain( session -> {
							assertThat( session.currentTransaction() )
									.as( "No transaction should be active before begin" )
									.isNull();

							ReactiveConnection connection = ( (MutinySessionImpl) session ).getReactiveConnection();
							return Uni.createFrom().completionStage( connection.beginTransaction() )
									.invoke( () -> {
										Mutiny.Transaction tx = session.currentTransaction();
										assertThat( tx )
												.as( "currentTransaction() should detect externally-opened transaction" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );

										assertThat( tx.isMarkedForRollback() ).isFalse();
										tx.markForRollback();
										assertThat( tx.isMarkedForRollback() ).isTrue();

										Mutiny.Transaction tx2 = session.currentTransaction();
										assertThat( tx2 )
												.as( "ExternalTransaction should be returned" )
												.isInstanceOf( ExternalTransaction.class );
									} )
									.chain( () -> Uni.createFrom().completionStage( connection.rollbackTransaction() ) )
									.invoke( () -> assertThat( session.currentTransaction() )
											.as( "currentTransaction() should return null after rollback" )
											.isNull()
									)
									.chain( session::close );
						} )
		);
	}

	@Test
	public void testStageCurrentTransactionDetectsExternalTransaction(VertxTestContext context) {
		test(
				context, getSessionFactory()
						.openSession()
						.thenCompose( session -> {
							assertThat( session.currentTransaction() )
									.as( "No transaction should be active before begin" )
									.isNull();

							ReactiveConnection connection = ( (StageSessionImpl) session ).getReactiveConnection();
							return connection.beginTransaction()
									.thenAccept( v -> {
										Stage.Transaction tx = session.currentTransaction();
										assertThat( tx )
												.as( "currentTransaction() should detect externally-opened transaction" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );

										assertThat( tx.isMarkedForRollback() ).isFalse();
										tx.markForRollback();
										assertThat( tx.isMarkedForRollback() ).isTrue();

										Stage.Transaction tx2 = session.currentTransaction();
										assertThat( tx2 )
												.as( "ExternalTransaction should be returned" )
												.isInstanceOf( ExternalTransaction.class );
									} )
									.thenCompose( v -> connection.rollbackTransaction() )
									.thenAccept( v -> assertThat( session.currentTransaction() )
											.as( "currentTransaction() should return null after rollback" )
											.isNull()
									)
									.thenCompose( v -> session.close() );
						} )
		);
	}

	@Test
	public void testMutinyWithTransactionJoinsExternalTransaction(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.openSession()
						.chain( session -> {
							ReactiveConnection connection = ( (MutinySessionImpl) session ).getReactiveConnection();
							return Uni.createFrom().completionStage( connection.beginTransaction() )
									.chain( () -> session.withTransaction( tx -> {
										assertThat( tx )
												.as( "withTransaction should join external transaction" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );
										return session
												.createSelectionQuery( "from Thing", Thing.class )
												.getResultList();
									} ) )
									.chain( () -> Uni.createFrom().completionStage( connection.rollbackTransaction() ) )
									.chain( session::close );
						} )
		);
	}

	@Test
	public void testStageWithTransactionJoinsExternalTransaction(VertxTestContext context) {
		test(
				context, getSessionFactory()
						.openSession()
						.thenCompose( session -> {
							ReactiveConnection connection = ( (StageSessionImpl) session ).getReactiveConnection();
							return connection.beginTransaction()
									.thenCompose( v -> session.withTransaction( tx -> {
										assertThat( tx )
												.as( "withTransaction should join external transaction" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );
										return session
												.createSelectionQuery( "from Thing", Thing.class )
												.getResultList();
									} ) )
									.thenCompose( v -> connection.rollbackTransaction() )
									.thenCompose( v -> session.close() );
						} )
		);
	}

	@Test
	public void testMutinyNewExternalTransactionResetsState(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.openSession()
						.chain( session -> {
							ReactiveConnection connection = ( (MutinySessionImpl) session ).getReactiveConnection();
							return Uni.createFrom().completionStage( connection.beginTransaction() )
									.invoke( () -> {
										Mutiny.Transaction tx = session.currentTransaction();
										assertThat( tx ).isNotNull();
										tx.markForRollback();
										assertThat( tx.isMarkedForRollback() ).isTrue();
									} )
									.chain( () -> Uni.createFrom().completionStage( connection.rollbackTransaction() ) )
									.invoke( () -> assertThat( session.currentTransaction() ).isNull() )
									// Start a second external transaction on the same connection
									.chain( () -> Uni.createFrom().completionStage( connection.beginTransaction() ) )
									.invoke( () -> {
										Mutiny.Transaction tx2 = session.currentTransaction();
										assertThat( tx2 )
												.as( "New external transaction should return a fresh instance" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );
										assertThat( tx2.isMarkedForRollback() )
												.as( "Fresh ExternalTransaction should not carry stale rollback state" )
												.isFalse();
									} )
									.chain( () -> Uni.createFrom().completionStage( connection.rollbackTransaction() ) )
									.chain( session::close );
						} )
		);
	}

	@Test
	public void testStageNewExternalTransactionResetsState(VertxTestContext context) {
		test(
				context, getSessionFactory()
						.openSession()
						.thenCompose( session -> {
							ReactiveConnection connection = ( (StageSessionImpl) session ).getReactiveConnection();
							return connection.beginTransaction()
									.thenAccept( v -> {
										Stage.Transaction tx = session.currentTransaction();
										assertThat( tx ).isNotNull();
										tx.markForRollback();
										assertThat( tx.isMarkedForRollback() ).isTrue();
									} )
									.thenCompose( v -> connection.rollbackTransaction() )
									.thenAccept( v -> assertThat( session.currentTransaction() ).isNull() )
									// Start a second external transaction on the same connection
									.thenCompose( v -> connection.beginTransaction() )
									.thenAccept( v -> {
										Stage.Transaction tx2 = session.currentTransaction();
										assertThat( tx2 )
												.as( "New external transaction should return a fresh instance" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );
										assertThat( tx2.isMarkedForRollback() )
												.as( "Fresh ExternalTransaction should not carry stale rollback state" )
												.isFalse();
									} )
									.thenCompose( v -> connection.rollbackTransaction() )
									.thenCompose( v -> session.close() );
						} )
		);
	}

	@Test
	public void testMutinyBackToBackExternalTransactionsResetState(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.openSession()
						.chain( session -> {
							ReactiveConnection connection = ( (MutinySessionImpl) session ).getReactiveConnection();
							return Uni.createFrom().completionStage( connection.beginTransaction() )
									.invoke( () -> {
										Mutiny.Transaction tx = session.currentTransaction();
										assertThat( tx ).isNotNull();
										tx.markForRollback();
										assertThat( tx.isMarkedForRollback() ).isTrue();
									} )
									.chain( () -> Uni.createFrom().completionStage( connection.rollbackTransaction() ) )
									// Start a second transaction WITHOUT calling currentTransaction() in between
									.chain( () -> Uni.createFrom().completionStage( connection.beginTransaction() ) )
									.invoke( () -> {
										Mutiny.Transaction tx2 = session.currentTransaction();
										assertThat( tx2 )
												.as( "New external transaction should not carry stale rollback state" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );
										assertThat( tx2.isMarkedForRollback() )
												.as( "Back-to-back external transaction must not inherit markedForRollback" )
												.isFalse();
									} )
									.chain( () -> Uni.createFrom().completionStage( connection.rollbackTransaction() ) )
									.chain( session::close );
						} )
		);
	}

	@Test
	public void testStageBackToBackExternalTransactionsResetState(VertxTestContext context) {
		test(
				context, getSessionFactory()
						.openSession()
						.thenCompose( session -> {
							ReactiveConnection connection = ( (StageSessionImpl) session ).getReactiveConnection();
							return connection.beginTransaction()
									.thenAccept( v -> {
										Stage.Transaction tx = session.currentTransaction();
										assertThat( tx ).isNotNull();
										tx.markForRollback();
										assertThat( tx.isMarkedForRollback() ).isTrue();
									} )
									.thenCompose( v -> connection.rollbackTransaction() )
									// Start a second transaction WITHOUT calling currentTransaction() in between
									.thenCompose( v -> connection.beginTransaction() )
									.thenAccept( v -> {
										Stage.Transaction tx2 = session.currentTransaction();
										assertThat( tx2 )
												.as( "New external transaction should not carry stale rollback state" )
												.isNotNull()
												.isInstanceOf( ExternalTransaction.class );
										assertThat( tx2.isMarkedForRollback() )
												.as( "Back-to-back external transaction must not inherit markedForRollback" )
												.isFalse();
									} )
									.thenCompose( v -> connection.rollbackTransaction() )
									.thenCompose( v -> session.close() );
						} )
		);
	}

	@Entity(name = "Thing")
	@Table(name = "external_tx_thing")
	public static class Thing {
		@Id
		public Integer id;
		public String name;

		public Thing() {
		}

		public Thing(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}
}

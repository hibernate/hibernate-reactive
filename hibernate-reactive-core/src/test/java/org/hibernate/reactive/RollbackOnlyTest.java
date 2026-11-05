/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.sqm.UnknownEntityException;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.NoResultException;
import jakarta.persistence.RollbackException;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

/**
 * Like in Hibernate ORM, when an operation fails, the transaction is marked for rollback only:
 * even if the exception is caught, the transaction won't be committed.
 * <p>
 * With JPA transaction compliance disabled (the default), the transaction is rolled back silently.
 *
 * @see RollbackOnlyJpaComplianceTest
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class RollbackOnlyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Item.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.JPA_TRANSACTION_COMPLIANCE, String.valueOf( jpaTransactionCompliance() ) );
		return configuration;
	}

	protected boolean jpaTransactionCompliance() {
		return false;
	}

	/**
	 * An entity with an id, but not in the database:
	 * {@code persist()} fails with "detached entity passed to persist".
	 */
	private static Item detached() {
		Item item = new Item( "detached" );
		item.id = 999L;
		return item;
	}

	/**
	 * The outcome of a transaction marked for rollback only, the only difference with
	 * {@link RollbackOnlyJpaComplianceTest}: with JPA transaction compliance enabled,
	 * the transaction fails with a {@link RollbackException}.
	 */
	private CompletionStage<?> rolledBackOnly(CompletionStage<?> transaction) {
		return jpaTransactionCompliance()
				? assertThrown( RollbackException.class, transaction )
				: transaction;
	}

	private Uni<?> rolledBackOnly(Uni<?> transaction) {
		return jpaTransactionCompliance()
				? assertThrown( RollbackException.class, transaction )
				: transaction;
	}

	private CompletionStage<List<Item>> findAllItems() {
		return getSessionFactory().withSession( s -> s
				.createSelectionQuery( "from Item", Item.class )
				.getResultList() );
	}

	@Test
	public void testRecoveredFailureWithStage(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, rolledBackOnly( getSessionFactory().withTransaction( s -> s
						.persist( detached() )
						// The application ignores the failure
						.handle( (v, e) -> null )
						.thenCompose( v -> s.persist( valid ) ) ) )
				.thenCompose( v -> findAllItems() )
				.thenAccept( items -> assertThat( items ).isEmpty() )
		);
	}

	@Test
	public void testRecoveredFailureWithMutiny(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, rolledBackOnly( getMutinySessionFactory().withTransaction( s -> s
						.persist( detached() )
						// The application ignores the failure
						.onFailure().recoverWithNull()
						.chain( () -> s.persist( valid ) ) ) )
				.chain( () -> Uni.createFrom().completionStage( findAllItems() ) )
				.invoke( items -> assertThat( items ).isEmpty() )
		);
	}

	@Test
	public void testIsMarkedForRollbackWithStage(VertxTestContext context) {
		test( context, rolledBackOnly( getSessionFactory().withTransaction( (s, tx) -> s
				.persist( detached() )
				.handle( (v, e) -> null )
				.thenAccept( v -> assertThat( tx.isMarkedForRollback() ).isTrue() ) ) )
		);
	}

	@Test
	public void testIsMarkedForRollbackWithMutiny(VertxTestContext context) {
		test( context, rolledBackOnly( getMutinySessionFactory().withTransaction( (s, tx) -> s
				.persist( detached() )
				.onFailure().recoverWithNull()
				.invoke( () -> assertThat( tx.isMarkedForRollback() ).isTrue() ) ) )
		);
	}

	@Test
	public void testNoResultExceptionDoesNotRollBackWithStage(VertxTestContext context) {
		// Same as Hibernate ORM, NoResultException doesn't mark the transaction for rollback
		Item valid = new Item( "valid" );
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( valid )
						.thenCompose( v -> s
								.createSelectionQuery( "from Item where name = 'missing'", Item.class )
								.getSingleResult() )
						.handle( (v, e) -> {
							assertThat( e ).hasCauseInstanceOf( NoResultException.class );
							assertThat( tx.isMarkedForRollback() ).isFalse();
							return null;
						} ) )
				.thenCompose( v -> findAllItems() )
				.thenAccept( items -> assertThat( items ).containsExactly( valid ) )
		);
	}

	@Test
	public void testNoResultExceptionDoesNotRollBackWithMutiny(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( valid )
						.chain( () -> s
								.createSelectionQuery( "from Item where name = 'missing'", Item.class )
								.getSingleResult() )
						.onFailure( NoResultException.class ).recoverWithNull()
						.invoke( () -> assertThat( tx.isMarkedForRollback() ).isFalse() ) )
				.chain( () -> Uni.createFrom().completionStage( findAllItems() ) )
				.invoke( items -> assertThat( items ).containsExactly( valid ) )
		);
	}

	@Test
	public void testNoResultExceptionWithNativeQueryDoesNotRollBackWithStage(VertxTestContext context) {
		// Like testNoResultExceptionDoesNotRollBackWithStage, but native queries convert the exceptions differently
		Item valid = new Item( "valid" );
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( valid )
						.thenCompose( v -> s
								.createNativeQuery( "select name from RollbackOnlyItem where name = 'missing'", String.class )
								.getSingleResult() )
						.handle( (v, e) -> {
							assertThat( e ).hasCauseInstanceOf( NoResultException.class );
							assertThat( tx.isMarkedForRollback() ).isFalse();
							return null;
						} ) )
				.thenCompose( v -> findAllItems() )
				.thenAccept( items -> assertThat( items ).containsExactly( valid ) )
		);
	}

	@Test
	public void testNoResultExceptionWithNativeQueryDoesNotRollBackWithMutiny(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( valid )
						.chain( () -> s
								.createNativeQuery( "select name from RollbackOnlyItem where name = 'missing'", String.class )
								.getSingleResult() )
						.onFailure( NoResultException.class ).recoverWithNull()
						.invoke( () -> assertThat( tx.isMarkedForRollback() ).isFalse() ) )
				.chain( () -> Uni.createFrom().completionStage( findAllItems() ) )
				.invoke( items -> assertThat( items ).containsExactly( valid ) )
		);
	}

	@Test
	public void testNextTransactionCommitsWithStage(VertxTestContext context) {
		// The flag only applies to the transaction that was marked for rollback
		Item valid = new Item( "valid" );
		test( context, openSession()
				.thenCompose( s -> rolledBackOnly( s
						.withTransaction( tx -> s.persist( detached() ).handle( (v, e) -> null ) ) )
						.thenCompose( v -> s.withTransaction( tx -> s.persist( valid ) ) )
				)
				.thenCompose( v -> findAllItems() )
				.thenAccept( items -> assertThat( items ).containsExactly( valid ) )
		);
	}

	@Test
	public void testNextTransactionCommitsWithMutiny(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, openMutinySession()
				.chain( s -> rolledBackOnly( s
						.withTransaction( tx -> s.persist( detached() ).onFailure().recoverWithNull() ) )
						.chain( () -> s.withTransaction( tx -> s.persist( valid ) ) )
				)
				.chain( () -> Uni.createFrom().completionStage( findAllItems() ) )
				.invoke( items -> assertThat( items ).containsExactly( valid ) )
		);
	}

	@Test
	public void testRecoveredFailureWithStatelessStage(VertxTestContext context) {
		// Like testRecoveredFailureWithStage, but with a stateless session: the failure comes
		// from the creation of a query, which marks the transaction for rollback only like in ORM
		Item valid = new Item( "valid" );
		test( context, rolledBackOnly( getSessionFactory().withStatelessTransaction( s -> {
					try {
						s.createSelectionQuery( "from NotAnEntity", Item.class );
					}
					catch (UnknownEntityException e) {
						// The application ignores the failure
					}
					return s.insert( valid );
				} ) )
				.thenCompose( v -> findAllItems() )
				.thenAccept( items -> assertThat( items ).isEmpty() )
		);
	}

	@Test
	public void testRecoveredFailureWithStatelessMutiny(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, rolledBackOnly( getMutinySessionFactory().withStatelessTransaction( s -> {
					try {
						s.createSelectionQuery( "from NotAnEntity", Item.class );
					}
					catch (UnknownEntityException e) {
						// The application ignores the failure
					}
					return s.insert( valid );
				} ) )
				.chain( () -> Uni.createFrom().completionStage( findAllItems() ) )
				.invoke( items -> assertThat( items ).isEmpty() )
		);
	}

	@Test
	public void testExplicitMarkForRollbackWithStage(VertxTestContext context) {
		// Rolling back explicitly never fails, even with JPA transaction compliance enabled
		Item valid = new Item( "valid" );
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( valid )
						.thenAccept( v -> tx.markForRollback() ) )
				.thenCompose( v -> findAllItems() )
				.thenAccept( items -> assertThat( items ).isEmpty() )
		);
	}

	@Test
	public void testExplicitMarkForRollbackWithMutiny(VertxTestContext context) {
		Item valid = new Item( "valid" );
		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( valid )
						.invoke( tx::markForRollback ) )
				.chain( () -> Uni.createFrom().completionStage( findAllItems() ) )
				.invoke( items -> assertThat( items ).isEmpty() )
		);
	}

	@Entity(name = "Item")
	@Table(name = "RollbackOnlyItem")
	static class Item {
		@Id
		@GeneratedValue
		Long id;

		String name;

		Item() {
		}

		Item(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof Item item && Objects.equals( name, item.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}
}

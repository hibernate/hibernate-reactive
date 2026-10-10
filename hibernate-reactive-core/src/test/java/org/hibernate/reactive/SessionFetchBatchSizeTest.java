/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Hibernate ORM 6.3 session-level batch/subselect fetching API:
 * <ul>
 *     <li>{@code Session.getFetchBatchSize()} / {@code Session.setFetchBatchSize(int)}</li>
 *     <li>{@code Session.isSubselectFetchingEnabled()} / {@code Session.setSubselectFetchingEnabled(boolean)}</li>
 * </ul>
 *
 * @see <a href="https://github.com/hibernate/hibernate-reactive/issues/1759">Issue #1759</a>
 * @see <a href="https://github.com/hibernate/hibernate-reactive/pull/1747">PR #1747</a>
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class SessionFetchBatchSizeTest extends BaseReactiveTest {

	// Static so it survives across PER_METHOD test instances; constructConfiguration() only runs
	// the first time the SessionFactory is built (see SessionFactoryManager#start).
	private static SqlStatementTracker sqlTracker;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Child.class, Parent.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker(
				SessionFetchBatchSizeTest::isChildSelect,
				configuration.getProperties()
		);
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean isChildSelect(String sql) {
		String lower = sql.toLowerCase();
		return lower.startsWith( "select" ) && lower.contains( "sfbchild" );
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		sqlTracker.clear();
		return getSessionFactory()
				.withTransaction( s -> s.createMutationQuery( "delete from Child" ).executeUpdate()
						.thenCompose( v -> s.createMutationQuery( "delete from Parent" ).executeUpdate() ) )
				.thenApply( v -> null );
	}

	@Test
	public void testGetAndSetFetchBatchSizeWithStageSession(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			s.setFetchBatchSize( 25 );
			assertThat( s.getFetchBatchSize() ).isEqualTo( 25 );

			s.setFetchBatchSize( 0 );
			assertThat( s.getFetchBatchSize() ).isEqualTo( 0 );

			s.setFetchBatchSize( -1 );
			assertThat( s.getFetchBatchSize() ).isEqualTo( -1 );

			return CompletionStages.voidFuture();
		} ) );
	}

	@Test
	public void testGetAndSetSubselectFetchingWithStageSession(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			s.setSubselectFetchingEnabled( true );
			assertThat( s.isSubselectFetchingEnabled() ).isTrue();

			s.setSubselectFetchingEnabled( false );
			assertThat( s.isSubselectFetchingEnabled() ).isFalse();

			return CompletionStages.voidFuture();
		} ) );
	}

	@Test
	public void testGetAndSetFetchBatchSizeWithMutinySession(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			s.setFetchBatchSize( 10 );
			assertThat( s.getFetchBatchSize() ).isEqualTo( 10 );

			s.setFetchBatchSize( 0 );
			assertThat( s.getFetchBatchSize() ).isEqualTo( 0 );

			s.setFetchBatchSize( -1 );
			assertThat( s.getFetchBatchSize() ).isEqualTo( -1 );

			return Uni.createFrom().voidItem();
		} ) );
	}

	@Test
	public void testGetAndSetSubselectFetchingWithMutinySession(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			s.setSubselectFetchingEnabled( true );
			assertThat( s.isSubselectFetchingEnabled() ).isTrue();

			s.setSubselectFetchingEnabled( false );
			assertThat( s.isSubselectFetchingEnabled() ).isFalse();

			return Uni.createFrom().voidItem();
		} ) );
	}

	@Test
	public void testBatchFetchingWithSessionLevelBatchSize(VertxTestContext context) {
		Parent parent1 = new Parent( "Parent1" );
		Parent parent2 = new Parent( "Parent2" );
		parent1.children.add( new Child( parent1 ) );
		parent1.children.add( new Child( parent1 ) );
		parent2.children.add( new Child( parent2 ) );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( parent1, parent2 ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					// Enable batch fetching at session level
					s.setFetchBatchSize( 10 );
					return s.createSelectionQuery( "from Parent p order by p.name", Parent.class )
							.getResultList()
							.thenCompose( list -> {
								assertThat( list ).hasSize( 2 );
								Parent p1 = list.get( 0 );
								Parent p2 = list.get( 1 );
								assertThat( Hibernate.isInitialized( p1.children ) ).isFalse();
								assertThat( Hibernate.isInitialized( p2.children ) ).isFalse();
								// Reset the tracker so we only inspect fetch-driven SELECTs against SFBChild
								sqlTracker.clear();
								return s.fetch( p1.children )
										.thenAccept( children -> {
											// Both collections must be initialized via a single batched SELECT
											assertThat( Hibernate.isInitialized( p1.children ) ).isTrue();
											assertThat( Hibernate.isInitialized( p2.children ) ).isTrue();
											assertThat( p1.children ).hasSize( 2 );
											assertThat( p2.children ).hasSize( 1 );

											List<String> queries = sqlTracker.getLoggedQueries();
											assertThat( queries )
													.as( "Expected exactly one SELECT against SFBChild for batch fetching" )
													.hasSize( 1 );
											// Different dialects bind the parent ids differently:
											//   - generic SQL: where parent_id in (?, ?, ...)
											//   - PostgreSQL:  where parent_id = any ($1)
											assertThat( queries.get( 0 ).toLowerCase() )
													.as( "Batch SELECT should bind multiple parent ids in one query" )
													.containsPattern( "in\\s*\\(|=\\s*any\\s*\\(" );
										} );
							} );
				} ) )
		);
	}

	@Test
	public void testSubselectFetchingWithSessionLevelSetting(VertxTestContext context) {
		Parent parent1 = new Parent( "Parent1" );
		Parent parent2 = new Parent( "Parent2" );
		parent1.children.add( new Child( parent1 ) );
		parent1.children.add( new Child( parent1 ) );
		parent2.children.add( new Child( parent2 ) );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( parent1, parent2 ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					// Enable subselect fetching at session level
					s.setSubselectFetchingEnabled( true );
					return s.createSelectionQuery( "from Parent p order by p.name", Parent.class )
							.getResultList()
							.thenCompose( list -> {
								assertThat( list ).hasSize( 2 );
								Parent p1 = list.get( 0 );
								Parent p2 = list.get( 1 );
								assertThat( Hibernate.isInitialized( p1.children ) ).isFalse();
								assertThat( Hibernate.isInitialized( p2.children ) ).isFalse();
								// Reset the tracker so we only inspect fetch-driven SELECTs against SFBChild
								sqlTracker.clear();
								return s.fetch( p1.children )
										.thenAccept( children -> {
											// One subselect must initialize both parents' collections
											assertThat( Hibernate.isInitialized( p1.children ) ).isTrue();
											assertThat( Hibernate.isInitialized( p2.children ) ).isTrue();
											assertThat( p1.children ).hasSize( 2 );
											assertThat( p2.children ).hasSize( 1 );

											List<String> queries = sqlTracker.getLoggedQueries();
											assertThat( queries )
													.as( "Expected exactly one SELECT against SFBChild for subselect fetching" )
													.hasSize( 1 );
											assertThat( queries.get( 0 ).toLowerCase() )
													.as( "Subselect SELECT should embed the parent query" )
													.containsPattern( "in\\s*\\(\\s*select" );
										} );
							} );
				} ) )
		);
	}

	@Entity(name = "Parent")
	@Table(name = "SFBParent")
	public static class Parent {
		@Id
		@GeneratedValue
		Integer id;

		String name;

		@OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "parent")
		List<Child> children = new ArrayList<>();

		public Parent() {
		}

		public Parent(String name) {
			this.name = name;
		}
	}

	@Entity(name = "Child")
	@Table(name = "SFBChild")
	public static class Child {
		@Id
		@GeneratedValue
		Integer id;

		@ManyToOne(fetch = FetchType.LAZY)
		Parent parent;

		public Child() {
		}

		public Child(Parent parent) {
			this.parent = parent;
		}
	}
}

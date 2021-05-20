/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;

import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

public class EmptyCompositeCollectionKeyTest extends BaseReactiveTest {
	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.skipTestsFor( DB2 );
	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.getProperties().put( Environment.CREATE_EMPTY_COMPOSITES_ENABLED, "true" );
		configuration.addAnnotatedClass( Family.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Family" ) );
	}

	@Test
	public void testGetEntityWithEmptyChildrenCollection(TestContext context) {
		/* CASE 1:  Family has Parent + child with null names + NULL relatives */
		Family family = new Family( 1, new Parent( new Child( null, null ) ) );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					context.assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					context.assertNull( foundFamily.parent.nickname );
					context.assertTrue( foundFamily.parent.children.isEmpty() );
					context.assertNull( foundFamily.parent.child.name );
					context.assertNull( foundFamily.parent.child.petname );
					context.assertTrue( Hibernate.isInitialized( foundFamily.relatives ) );
					context.assertTrue( foundFamily.relatives.isEmpty() );
				} )
		);
	}

	@Test
	public void testGetEntityWithParentNullChild(TestContext context) {
		/* CASE 2:  Family has Parent + child with null names + NULL relatives */
		Family family = new Family( 2, new Parent() );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					context.assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					context.assertNull( foundFamily.parent.nickname );
					context.assertTrue( foundFamily.parent.children.isEmpty() );
					context.assertNotNull( foundFamily.parent.child );
					context.assertNull( foundFamily.parent.child.petname );
					context.assertTrue( foundFamily.relatives.isEmpty() );
				} )
		);
	}

	@Test
	public void testGetEntityWithNullParentNullChild(TestContext context) {
		/* CASE 3:  Parent and children are all null */
		Family family = new Family( 3, null );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					context.assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					context.assertNull( foundFamily.parent.nickname );
					context.assertTrue( foundFamily.parent.children.isEmpty() );
					context.assertNotNull( foundFamily.parent.child );
					context.assertNull( foundFamily.parent.child.name );
					context.assertTrue( foundFamily.relatives.isEmpty() );
				} )
		);
	}


	@Test
	public void testGetEntityWithNullParentNullChildAndRelatives(TestContext context) {
		/* CASE 4:  Parent and children are all null and relatives set exists */
		Set<Child> relatives = new HashSet<>();
		relatives.add( new Child() );
		Family family = new Family( 4, null, relatives );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					context.assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					context.assertNull( foundFamily.parent.nickname );
					context.assertTrue( foundFamily.parent.children.isEmpty() );
					context.assertNotNull( foundFamily.parent.child );
					context.assertNull( foundFamily.parent.child.petname );
					context.assertFalse( foundFamily.relatives.isEmpty() );
					context.assertNull( foundFamily.relatives.iterator().next().name );
				} )
		);
	}

	@Entity(name  = "Family")
	public static class Family {
		@Id
		private Integer id;

		private Parent  parent;

		@ElementCollection(fetch = FetchType.EAGER)
		private Set<Child> relatives;

		public Family() {
			super();
		}

		public Family(Integer id, Parent parent) {
			this.id = id;
			this.parent = parent;
		}

		public Family(Integer id, Parent parent, Set<Child> relatives) {
			this.id = id;
			this.parent = parent;
			this.relatives = relatives;
		}
	}

	@Embeddable
	public static class Parent {
		private Child child;
		private String nickname;

		@ElementCollection(fetch = FetchType.EAGER)
		List<Child> children;

		public Parent() {
			super();
		}

		public Parent(Child child) {
			this.child = child;
		}
	}

	@Embeddable
	public static class Child {
		private String name;
		private String petname;

		public Child() {
			super();
		}

		public Child(String name, String petname) {
			this.name = name;
			this.petname = petname;
		}
	}
}

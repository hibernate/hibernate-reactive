/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class EmptyCompositeCollectionKeyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Family.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.getProperties().put( Environment.CREATE_EMPTY_COMPOSITES_ENABLED, "true" );
		return configuration;
	}

	@Test
	public void testGetEntityWithEmptyChildrenCollection(VertxTestContext context) {
		/* CASE 1:  Family has Parent + child with null names + NULL relatives */
		Family family = new Family( 1, new Parent( new Child( null, null ) ) );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					assertNull( foundFamily.parent.nickname );
					assertTrue( foundFamily.parent.children.isEmpty() );
					assertNull( foundFamily.parent.child.name );
					assertNull( foundFamily.parent.child.petname );
					assertTrue( Hibernate.isInitialized( foundFamily.relatives ) );
					assertTrue( foundFamily.relatives.isEmpty() );
				} )
		);
	}

	@Test
	public void testGetEntityWithParentNullChild(VertxTestContext context) {
		/* CASE 2:  Family has Parent + child with null names + NULL relatives */
		Family family = new Family( 2, new Parent() );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					assertNull( foundFamily.parent.nickname );
					assertTrue( foundFamily.parent.children.isEmpty() );
					assertNotNull( foundFamily.parent.child );
					assertNull( foundFamily.parent.child.petname );
					assertTrue( foundFamily.relatives.isEmpty() );
				} )
		);
	}

	@Test
	public void testGetEntityWithNullParentNullChild(VertxTestContext context) {
		/* CASE 3:  Parent and children are all null */
		Family family = new Family( 3, null );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( family )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Family.class, family.id ) )
				.thenAccept( foundFamily -> {
					assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					assertNull( foundFamily.parent.nickname );
					assertTrue( foundFamily.parent.children.isEmpty() );
					assertNotNull( foundFamily.parent.child );
					assertNull( foundFamily.parent.child.name );
					assertTrue( foundFamily.relatives.isEmpty() );
				} )
		);
	}


	@Test
	public void testGetEntityWithNullParentNullChildAndRelatives(VertxTestContext context) {
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
					assertTrue( Hibernate.isInitialized( foundFamily.parent.children ) );
					assertNull( foundFamily.parent.nickname );
					assertTrue( foundFamily.parent.children.isEmpty() );
					assertNotNull( foundFamily.parent.child );
					assertNull( foundFamily.parent.child.petname );
					assertFalse( foundFamily.relatives.isEmpty() );
					assertNull( foundFamily.relatives.iterator().next().name );
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

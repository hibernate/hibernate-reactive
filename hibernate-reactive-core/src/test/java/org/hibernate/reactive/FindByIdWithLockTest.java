/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
public class FindByIdWithLockTest extends BaseReactiveTest {
	private static final Long CHILD_ID = 1L;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Parent.class, Child.class );
	}

	@Test
	public void testFindChild(VertxTestContext context) {
		Parent parent = new Parent( 1L, "Lio" );
		Child child = new Child( CHILD_ID, "And" );
		test(
				context, getMutinySessionFactory()
						.withTransaction( session -> session.persistAll( parent, child ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session
										.find( Child.class, CHILD_ID, LockModeType.PESSIMISTIC_WRITE )
										.invoke( c -> {
													 assertThat( c ).isNotNull();
													 assertThat( c.getId() ).isEqualTo( CHILD_ID );
												 }
										) ) )
		);
	}

	@Entity(name = "Parent")
	public static class Parent {

		@Id
		private Long id;

		private String name;

		@OneToMany(fetch = FetchType.EAGER)
		public List<Child> children;

		public Parent() {
		}

		public Parent(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public void add(Child child) {
			if ( children == null ) {
				children = new ArrayList<>();
			}
			children.add( child );
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public List<Child> getChildren() {
			return children;
		}
	}


	@Entity(name = "Child")
	public static class Child {

		@Id
		private Long id;

		public String name;

		@ManyToOne
		public Parent parent;

		public Child() {
		}

		public Child(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Child(Long id, String name, Parent parent) {
			this.id = id;
			this.name = name;
			this.parent = parent;
			parent.add( this );
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Parent getParent() {
			return parent;
		}
	}


}

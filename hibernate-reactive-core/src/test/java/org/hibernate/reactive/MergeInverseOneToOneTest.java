/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.Hibernate;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Merge an entity with an inverse {@code @OneToOne(mappedBy = ...)} without cascade:
 * the association references the unique key of the child, which is itself an association.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class MergeInverseOneToOneTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		// The children first, so that cleanDb() deletes them before their parents
		return List.of( Child.class, Parent.class );
	}

	@Test
	public void testMergeDetachedEntity(VertxTestContext context) {
		final Parent parent = new Parent( 1L, "before" );
		final Child child = new Child( 2L, "child", parent );
		parent.child = child;
		final Parent expected = new Parent( 1L, "after" );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( parent, child ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Parent.class, parent.id ) ) )
				.invoke( detached -> detached.name = expected.name )
				.chain( detached -> getMutinySessionFactory().withTransaction( s -> s.merge( detached ) ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Parent.class, parent.id ) ) )
				.invoke( found -> {
					assertThat( found ).isEqualTo( expected );
					assertThat( found.child ).isEqualTo( child );
				} )
		);
	}

	// Unlike testMergeDetachedEntity, the child is an uninitialized proxy
	@Test
	public void testMergeDetachedEntityWithChildProxy(VertxTestContext context) {
		final Parent parent = new Parent( 1L, "before" );
		final Child child = new Child( 2L, "child", parent );
		parent.child = child;
		final Parent expected = new Parent( 1L, "after" );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( parent, child ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Parent.class, parent.id ) ) )
				.chain( detached -> getMutinySessionFactory()
						.withSession( s -> {
							final Child reference = s.getReference( Child.class, child.id );
							assertThat( Hibernate.isInitialized( reference ) ).isFalse();
							detached.child = reference;
							detached.name = expected.name;
							return Uni.createFrom().item( detached );
						} ) )
				.chain( detached -> getMutinySessionFactory().withTransaction( s -> s.merge( detached ) ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Parent.class, parent.id ) ) )
				.invoke( found -> {
					assertThat( found ).isEqualTo( expected );
					assertThat( found.child ).isEqualTo( child );
				} )
		);
	}

	@Entity(name = "Parent")
	@Table(name = "merge_inverse_parent")
	public static class Parent {
		@Id
		public Long id;

		public String name;

		@OneToOne(mappedBy = "parent", fetch = FetchType.EAGER)
		public Child child;

		public Parent() {
		}

		public Parent(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			return Objects.equals( name, ( (Parent) o ).name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}

	@Entity(name = "Child")
	@Table(name = "merge_inverse_child")
	public static class Child {
		@Id
		public Long id;

		public String name;

		@OneToOne(fetch = FetchType.EAGER)
		@JoinColumn(name = "parent_id", nullable = false, unique = true)
		public Parent parent;

		public Child() {
		}

		public Child(Long id, String name, Parent parent) {
			this.id = id;
			this.name = name;
			this.parent = parent;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			return Objects.equals( name, ( (Child) o ).name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}
}

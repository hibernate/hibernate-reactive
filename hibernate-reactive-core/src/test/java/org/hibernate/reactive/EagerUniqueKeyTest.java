/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;

import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import org.junit.Ignore;
import org.junit.Test;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * @see LazyUniqueKeyTest
 */
public class EagerUniqueKeyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Foo.class, Bar.class );
	}

	@Test
	public void testFindJoin(TestContext context) {
		Foo foo = new Foo( new Bar( "unique" ) );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( foo )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.clear() )
						.thenCompose( v -> session.find( Foo.class, foo.id ) )
						.thenAccept( result -> {
							context.assertTrue( Hibernate.isInitialized( result.getBar() ) );
							context.assertEquals( "unique", result.getBar().getKey() );
						} )
				) );
	}


	@Test
	public void testMergeDetached(TestContext context) {
		Bar bar = new Bar( "unique2" );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( bar ) )
				.thenCompose( i -> getSessionFactory()
						.withTransaction( session -> session.merge( new Foo( bar ) ) ) )
				.thenCompose( result -> getSessionFactory()
						.withTransaction( session -> session.fetch( result.getBar() )
								.thenAccept( b -> context.assertEquals( "unique2", b.getKey() ) )
						) ) );
	}

	@Ignore // This also fails in ORM
	@Test
	public void testMergeReference(TestContext context) {
		Bar bar = new Bar( "unique3" );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( bar ) )
				.thenCompose( i -> getSessionFactory()
						.withTransaction( session -> session
								.merge( new Foo( session.getReference( Bar.class, bar.getId() ) ) ) ) )
				.thenCompose( result -> getSessionFactory().withTransaction( session -> session.fetch( result.getBar() )
						.thenAccept( b -> context.assertEquals( "unique3", b.getKey() ) )
				) )
		);
	}

	@Test
	public void testPersistWithReference(TestContext context) {
		Bar bar = new Bar( "uniquePersist" );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( bar ) )
				.thenCompose( i -> getSessionFactory()
						.withTransaction( session -> {
							Foo foo = new Foo( session.getReference( Bar.class, bar.getId() ) );
							return session.persist( foo ).thenApply( v -> foo );
						} ) )
				.thenCompose( result -> getSessionFactory().withTransaction( session -> session.fetch( result.getBar() ) ) )
				.thenAccept( b -> context.assertEquals( "uniquePersist", b.getKey() ) )
		);
	}

	@Entity(name = "Foo")
	static class Foo {
		Foo(Bar bar) {
			this.bar = bar;
		}

		Foo() {
		}

		@Id
		@GeneratedValue
		long id;
		@ManyToOne(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
		@Fetch(FetchMode.JOIN)
		@JoinColumn(name = "bar_key", referencedColumnName = "nat_key")
		Bar bar;

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public Bar getBar() {
			return bar;
		}

		public void setBar(Bar bar) {
			this.bar = bar;
		}
	}

	@Entity(name = "Bar")
	static class Bar implements Serializable {
		Bar(String key) {
			this.key = key;
		}

		Bar() {
		}

		@Id
		@GeneratedValue
		long id;
		@Column(name = "nat_key", unique = true)
		String key;

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}
}

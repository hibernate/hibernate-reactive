/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @see EagerUniqueKeyTest
 */
public class LazyUniqueKeyTest extends BaseReactiveTest {

	// Db2: java.lang.IllegalStateException: Needed to have 6 in buffer but only had 0. In JDBC we would normally block here
	@RegisterExtension
	public DBSelectionExtension skip = skipTestsFor( DB2 );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Foo.class, Bar.class );
	}

	@Test
	public void testFindSelect(VertxTestContext context) {
		Foo foo = new Foo( new Bar( "unique" ) );
		test( context, getSessionFactory()
				.withTransaction( session -> session
						.persist( foo )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.clear() )
						.thenCompose( v -> session.find( Foo.class, foo.id ) )
//                        .thenApply( result -> {
//                            assertFalse( Hibernate.isInitialized(result.bar) );
//                            return result;
//                        } )
						.thenCompose( result -> session.fetch( result.bar ) )
						.thenAccept( bar -> assertEquals( "unique", bar.key ) ) )
		);
	}

	@Test
	public void testMergeDetached(VertxTestContext context) {
		Bar bar = new Bar( "unique2" );
		test( context, getSessionFactory()
				.withTransaction( (session, tx) -> session.persist( bar ) )
				.thenCompose( i -> getSessionFactory()
						.withTransaction( session -> session.merge( new Foo( bar ) ) ) )
				.thenCompose( result -> getSessionFactory()
						.withTransaction( session -> session.fetch( result.bar )
								.thenAccept( b -> assertEquals( "unique2", b.key ) )
				) ) );
	}

	@Disabled // see https://github.com/hibernate/hibernate-reactive/issues/1504
	@Test
	public void testMergeReference(VertxTestContext context) {
		Bar bar = new Bar( "unique3" );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( bar ) )
				.thenCompose( i -> getSessionFactory()
						.withTransaction( session-> session.merge( new Foo( session.getReference( Bar.class, bar.id ) ) ) )
				)
				.thenCompose( result -> getSessionFactory()
						.withTransaction( session-> session.fetch( result.bar )
						.thenAccept( b -> assertEquals( "unique3", b.key ) )
				) ) );
	}

	@Test
	public void testPersistReference(VertxTestContext context) {
		Bar bar = new Bar( "unique3" );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( bar ) )
				.thenCompose( i -> getSessionFactory()
						.withTransaction( session-> {
							Foo foo = new Foo( session.getReference( Bar.class, bar.id ) );
							return session.persist( foo ).thenApply( v -> foo );
						} )
				)
				.thenCompose( result -> getSessionFactory()
						.withTransaction( session-> session.fetch( result.bar )
								.thenAccept( b -> assertEquals( "unique3", b.getKey() ) )
						) ) );
	}

	@Entity(name = "Foo")
	static class Foo {
		Foo(Bar bar) {
			this.bar = bar;
		}

		Foo() {
		}

		@GeneratedValue
		@Id
		long id;
		@ManyToOne(cascade = CascadeType.PERSIST, fetch = FetchType.LAZY)
		@Fetch(FetchMode.SELECT)
		@JoinColumn(name = "bar_key", referencedColumnName = "nat_key")
		Bar bar;
	}

	@Entity(name = "Bar")
	static class Bar implements Serializable {
		Bar(String key) {
			this.key = key;
		}

		Bar() {
		}

		@GeneratedValue
		@Id
		long id;
		@Column(name = "nat_key", unique = true)
		String key;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}
}

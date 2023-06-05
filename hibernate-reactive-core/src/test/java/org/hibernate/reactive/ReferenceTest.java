/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class ReferenceTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class, Book.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return getSessionFactory()
				.withTransaction( s -> loop( entities, entityClass -> s
						.createQuery( "from " + entityName( entityClass ), entityClass )
						.getResultList()
						.thenCompose( list -> loop( list, s::remove ) ) ) );
	}

	private String entityName(Class<?> entityClass) {
		if ( Author.class.equals( entityClass ) ) {
			return "Writer";
		}
		return "Tome";
	}

	@Test
	public void testDetachedEntityReference(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> {
							Book book = s.getReference( Book.class, goodOmens.getId() );
							assertFalse( Hibernate.isInitialized( book ) );
							return s.persist( new Author( "Neil Gaiman", book ) )
									.thenCompose( v -> s.flush() );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> {
							Book book = s.getReference( goodOmens );
							assertFalse( Hibernate.isInitialized( book ) );
							return s.persist( new Author( "Terry Pratchett", book ) )
									.thenCompose( v -> s.flush() );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> {
							Book book = s.getReference( goodOmens );
							assertFalse( Hibernate.isInitialized( book ) );
							return Stage.fetch( book ).thenCompose( v -> Stage.fetch( book.getAuthors() ) );
						} )
						.thenAccept( optionalAssociation -> {
							assertTrue( Hibernate.isInitialized( optionalAssociation ) );
							assertNotNull( optionalAssociation );
							assertEquals( 2, optionalAssociation.size() );
						} )
		);
	}

	@Test
	public void testDetachedProxyReference(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference( goodOmens );
							assertFalse( Hibernate.isInitialized( reference ) );
							return openSession()
									.thenCompose( s -> {
										Book book = s.getReference( Book.class, reference.getId() );
										assertFalse( Hibernate.isInitialized( book ) );
										assertFalse( Hibernate.isInitialized( reference ) );
										return s.persist( new Author( "Neil Gaiman", book ) )
												.thenCompose( v -> s.flush() );
									} )
									.thenCompose( v -> openSession() )
									.thenCompose( s -> {
										Book book = s.getReference( reference );
										assertFalse( Hibernate.isInitialized( book ) );
										assertFalse( Hibernate.isInitialized( reference ) );
										return s.persist( new Author( "Terry Pratchett", book ) )
												.thenCompose( v -> s.flush() );
									} )
									.thenCompose( v -> openSession() )
									.thenCompose( s -> {
										Book book = s.getReference( reference );
										assertFalse( Hibernate.isInitialized( book ) );
										assertFalse( Hibernate.isInitialized( reference ) );
										return Stage.fetch( book ).thenCompose( v -> Stage.fetch( book.getAuthors() ) );
									} )
									.thenAccept( optionalAssociation -> {
										assertTrue( Hibernate.isInitialized( optionalAssociation ) );
										assertNotNull( optionalAssociation );
										assertEquals( 2, optionalAssociation.size() );
									} );
						} )
		);
	}

	@Test
	public void testRemoveDetachedProxy(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference( goodOmens );
							assertFalse( Hibernate.isInitialized( reference ) );
							return openSession()
									.thenCompose( s -> s.remove( s.getReference( reference ) )
											.thenCompose( v -> s.flush() ) );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> sess.find( Book.class, goodOmens.getId() ) )
						.thenAccept( Assertions::assertNull )
		);
	}

	@Test
	public void testRemoveWithTransaction(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( goodOmens ) )
				.call( () -> getMutinySessionFactory()
						.withSession( s -> s.find( Book.class, goodOmens.getId() ) )
						.invoke( book -> assertEquals( goodOmens, book ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.remove( s.getReference( Book.class, goodOmens.getId() ) ) ) )
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( Book.class, goodOmens.getId() ) ) )
				.invoke( Assertions::assertNull )
		);
	}

	@Test
	public void testLockDetachedProxy(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference( goodOmens );
							assertFalse( Hibernate.isInitialized( reference ) );
							return openSession().thenCompose( s -> s
									.lock( s.getReference( reference ), LockMode.PESSIMISTIC_FORCE_INCREMENT )
									.thenCompose( v -> s.flush() ) );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> sess.find( Book.class, goodOmens.getId() ) )
						.thenAccept( book -> {
							assertNotNull( book );
							assertEquals( 2, book.version );
						} )
		);
	}

	@Test
	public void testRefreshDetachedProxy(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference( goodOmens );
							assertFalse( Hibernate.isInitialized( reference ) );
							return openSession()
									.thenCompose( s -> s.refresh( s.getReference( reference ) )
											.thenAccept( v -> assertTrue( Hibernate.isInitialized( s.getReference( reference ) ) ) )
											.thenCompose( v -> s.flush() ) );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> sess.find( Book.class, goodOmens.getId() ) )
						.thenAccept( book -> {
							assertNotNull( book );
							assertEquals( 1, book.version );
						} )
		);
	}

	@Entity(name = "Tome")
	@Table(name = "TBook")
	public static class Book {

		@Id
		@GeneratedValue
		private Integer id;
		@Version
		private Integer version = 1;
		private String title;

		@OneToMany(fetch = FetchType.LAZY, mappedBy = "book")
		private List<Author> authors = new ArrayList<>();

		public Book() {
		}

		public Book(String title) {
			this.title = title;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public List<Author> getAuthors() {
			return authors;
		}

		public void setAuthors(List<Author> authors) {
			this.authors = authors;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Book book = (Book) o;
			return Objects.equals( title, book.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( title );
		}
	}

	@Entity(name = "Writer")
	@Table(name = "TAuthor")
	public static class Author {

		@Id
		@GeneratedValue
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.LAZY, optional = false)
		private Book book;

		public Author() {
		}

		public Author(String name, Book book) {
			this.name = name;
			this.book = book;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Book getBook() {
			return book;
		}

		public void setBook(Book book) {
			this.book = book;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Author author = (Author) o;
			return Objects.equals( name, author.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;
import org.junit.Test;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class ReferenceTest extends BaseReactiveTest {

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@Test
	public void testDetachedEntityReference(TestContext context) {
		final Book goodOmens = new Book("Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens).thenCompose(v -> s.flush()) )
						.thenCompose( v -> openSession() )
						.thenCompose(s -> {
							Book book = s.getReference(Book.class, goodOmens.getId());
							context.assertFalse( Hibernate.isInitialized(book) );
							return s.persist( new Author("Neil Gaiman", book) )
									.thenCompose( v -> s.flush() );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> {
							Book book = s.getReference(goodOmens);
							context.assertFalse( Hibernate.isInitialized(book) );
							return s.persist( new Author("Terry Pratchett", book) )
									.thenCompose( v -> s.flush() );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> {
							Book book = s.getReference(goodOmens);
							context.assertFalse( Hibernate.isInitialized(book) );
							return Stage.fetch(book).thenCompose( v -> Stage.fetch( book.getAuthors() ) );
						} )
						.thenAccept( optionalAssociation -> {
							context.assertTrue( Hibernate.isInitialized(optionalAssociation) );
							context.assertNotNull(optionalAssociation);
							context.assertEquals( 2, optionalAssociation.size() );
						} )
		);
	}

	@Test
	public void testDetachedProxyReference(TestContext context) {
		final Book goodOmens = new Book("Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens).thenCompose(v -> s.flush()) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference(goodOmens);
							context.assertFalse( Hibernate.isInitialized(reference) );
							return openSession()
									.thenCompose(s -> {
										Book book = s.getReference(Book.class, reference.getId());
										context.assertFalse( Hibernate.isInitialized(book) );
										context.assertFalse( Hibernate.isInitialized(reference) );
										return s.persist( new Author("Neil Gaiman", book) )
												.thenCompose( v -> s.flush() );
									} )
									.thenCompose( v -> openSession() )
									.thenCompose( s -> {
										Book book = s.getReference(reference);
										context.assertFalse( Hibernate.isInitialized(book) );
										context.assertFalse( Hibernate.isInitialized(reference) );
										return s.persist( new Author("Terry Pratchett", book) )
												.thenCompose( v -> s.flush() );
									} )
									.thenCompose( v -> openSession() )
									.thenCompose( s -> {
										Book book = s.getReference(reference);
										context.assertFalse( Hibernate.isInitialized(book) );
										context.assertFalse( Hibernate.isInitialized(reference) );
										return Stage.fetch(book).thenCompose( v -> Stage.fetch( book.getAuthors() ) );
									} )
									.thenAccept( optionalAssociation -> {
										context.assertTrue( Hibernate.isInitialized(optionalAssociation) );
										context.assertNotNull(optionalAssociation);
										context.assertEquals( 2, optionalAssociation.size() );
									} );
						} )
		);
	}

	@Test
	public void testRemoveDetachedProxy(TestContext context) {
		final Book goodOmens = new Book("Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference(goodOmens);
							context.assertFalse( Hibernate.isInitialized(reference) );
							return openSession()
									.thenCompose( s -> s.remove( s.getReference(reference) )
											.thenCompose( v -> s.flush() ) );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> sess.find( Book.class, goodOmens.getId() ) )
						.thenAccept( context::assertNull )
		);
	}

	@Test
	public void testLockDetachedProxy(TestContext context) {
		final Book goodOmens = new Book("Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference(goodOmens);
							context.assertFalse( Hibernate.isInitialized(reference) );
							return openSession()
									.thenCompose( s -> s.lock( s.getReference(reference), LockMode.PESSIMISTIC_FORCE_INCREMENT )
											.thenCompose( v -> s.flush() ) );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> sess.find( Book.class, goodOmens.getId() ) )
						.thenAccept( book -> {
							context.assertNotNull(book);
							context.assertEquals( 2, book.version );
						} )
		);
	}

	@Test
	public void testRefreshDetachedProxy(TestContext context) {
		final Book goodOmens = new Book("Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> {
							Book reference = sess.getReference(goodOmens);
							context.assertFalse( Hibernate.isInitialized(reference) );
							return openSession()
									.thenCompose( s -> s.refresh( s.getReference(reference) )
											.thenAccept( v -> context.assertTrue( Hibernate.isInitialized( s.getReference(reference) ) ) )
											.thenCompose( v -> s.flush() ) );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( sess -> sess.find( Book.class, goodOmens.getId() ) )
						.thenAccept( book -> {
							context.assertNotNull(book);
							context.assertEquals( 1, book.version );
						} )
		);
	}

	@Entity(name =  "Tome")
	@Table(name = "TBook")
	public static class Book {

		@Id @GeneratedValue
		private Integer id;
		@Version
		private Integer version = 1;
		private String title;

		@OneToMany(fetch = FetchType.LAZY, mappedBy="book")
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

		@Id @GeneratedValue
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

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.*;
import java.util.Date;
import java.util.Objects;

public class SecondaryTableTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SecondaryTableTest.Book.class );
		configuration.addAnnotatedClass( SecondaryTableTest.Author.class );
		return configuration;
	}

	@Test
	public void testRootClassViaAssociation(TestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date(), false);
		final Author author = new Author( "Charlie Mackesy", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book ) )
						.thenCompose( s -> s.persist( author ) )
						.thenCompose( s -> s.flush() )
						.thenCompose( v -> openSession())
						.thenCompose( s2 -> s2.find( Author.class, author.getId() ) )
						.thenAccept( auth -> {
							context.assertNotNull( auth );
							context.assertEquals( author, auth );
							context.assertEquals( book.getTitle(), auth.getBook().getTitle()  );
							context.assertFalse(book.isForbidden());
						} )
		);
	}

	@Test
	public void testSubclassViaAssociation(TestContext context) {
		final Book book = new Book( 6, "Necronomicon", new Date(), true);
		final Author author = new Author( "Abdul Alhazred", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book ))
						.thenCompose( s -> s.persist( author ) )
						.thenCompose( s -> s.flush() )
						.thenCompose( s -> s.find( Author.class, author.getId() ) )
						.thenAccept( auth -> {
							context.assertNotNull( auth );
							context.assertEquals( author, auth );
							context.assertEquals( book.getTitle(), auth.getBook().getTitle()  );
							context.assertTrue(book.isForbidden());
						} )
		);
	}

	@Test
	public void testRootClassViaFind(TestContext context) {

		final Book novel = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date(), false);
		final Author author = new Author( "Charlie Mackesy", novel );

		test( context,
				openSession()
						.thenCompose(s -> s.persist(novel))
						.thenCompose(s -> s.persist(author))
						.thenCompose(s -> s.flush())
						.thenCompose( v -> openSession())
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							context.assertNotNull(book);
							context.assertFalse(book.isForbidden());
							context.assertEquals(book.getTitle(), "The Boy, The Mole, The Fox and The Horse");
						}));
	}

	@Test
	public void testSubclassViaFind(TestContext context) {
		final Book spells = new Book( 6, "Necronomicon", new Date(), true);
		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				openSession()
						.thenCompose(s -> s.persist(spells))
						.thenCompose(s -> s.persist(author))
						.thenCompose(s -> s.flush())
						.thenCompose( v -> openSession())
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							context.assertNotNull(book);
							context.assertTrue(book.isForbidden());
							context.assertEquals(book.getTitle(), "Necronomicon");
						}));
	}

	@Entity
	@Table(name = Book.TABLE1)
	@SecondaryTable(name = Book.TABLE2)
	@SecondaryTable(name = Book.TABLE3)
	public static class Book {
		public static final String TABLE1 = "Book";
		public static final String TABLE2 = "SpellBook";
		public static final String TABLE3 = "Extra";

		@Id private Integer id;
		private String title;

		@Temporal(TemporalType.DATE)
		@Column(table = Book.TABLE2)
		private Date published;
		@Column(table = Book.TABLE2)
		private boolean forbidden;

		public Book() {
		}

		public Book(Integer id, String title, Date published, boolean forbidden) {
			this.id = id;
			this.title = title;
			this.published = published;
			this.forbidden = forbidden;
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

		public Date getPublished() {
			return published;
		}

		public void setPublished(Date published) {
			this.published = published;
		}

		public boolean isForbidden() {
			return forbidden;
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

	@Entity
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author";

		@Id @GeneratedValue
		private Integer id;
		private String name;

		@ManyToOne
		private Book book;

		public Author() {
		}

		public Author(String name, Book book) {
			this.name = name;
			this.book = book;
		}

		public Author(Integer id, String name, Book book) {
			this.id = id;
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

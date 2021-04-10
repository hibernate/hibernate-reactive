/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.skipTestsFor;


public class EagerOneToOneAssociationTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule db2WithVertx4BugRule = skipTestsFor( DB2 );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( Book.class, Author.class ) );
	}

	@Test
	public void testPersist(TestContext context) {
		final Book mostPopularBook = new Book( 5, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 3, "Charlie Mackesy" );
		mostPopularBook.setAuthor( author );
		author.setMostPopularBook( mostPopularBook );

		test(
				context, getSessionFactory()
						.withTransaction( (session, transaction) -> session.persist( mostPopularBook, author ) )
						.thenCompose( v -> getSessionFactory().withSession( session -> session.find( Book.class, 5 ) ) )
						.thenAccept( book -> context.assertEquals( mostPopularBook, book ) )
		);
	}

	@Entity
	@Table(name = "Book2")
	public static class Book {
		@Id
		private Integer id;
		private String title;

		@OneToOne(fetch = FetchType.EAGER)
		Author author;

		public Book() {
		}

		public Book(Integer id, String title) {
			this.id = id;
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

		public Author getAuthor() {
			return author;
		}

		public void setAuthor(Author author) {
			this.author = author;
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
	@Table(name = "Author2")
	public static class Author {

		@Id
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.EAGER)
		private Book mostPopularBook;

		public Author(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Author() {
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

		public Book getMostPopularBook() {
			return mostPopularBook;
		}

		public void setMostPopularBook(Book mostPopularBook) {
			this.mostPopularBook = mostPopularBook;
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

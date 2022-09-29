/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;


import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class EagerOneToOneAssociationTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void testPersist(TestContext context) {
		final Book mostPopularBook = new Book( 5, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 3, "Charlie Mackesy" );
		mostPopularBook.setAuthor( author );
		author.setMostPopularBook( mostPopularBook );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( mostPopularBook )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, 5 ) )
						.thenAccept(context::assertNotNull)
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

		public Book() {}

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

		public Author() {}

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

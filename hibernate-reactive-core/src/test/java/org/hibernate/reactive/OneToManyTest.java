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

import org.hibernate.Hibernate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OneToManyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void testPersistAll(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );
		final Book[] bookArray = new Book[2];
		bookArray[0] = book1;
		bookArray[1] = book2;

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> assertFalse( Hibernate.isInitialized( a.getBooks() ) ) )
						.chain( a -> session.fetch( a.getBooks() ) )
						.invoke( books -> assertThat( books ).containsExactlyInAnyOrder( book1, book2 ) )
				) )
		);
	}

	@Test
	public void testFetchJoinQueryGetSingleResult(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );
		final Book[] bookArray = new Book[2];
		bookArray[0] = book1;
		bookArray[1] = book2;

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( "select distinct a from Author a left join fetch a.books", Author.class )
						.getSingleResult()
						.invoke( a -> assertTrue( Hibernate.isInitialized( a.getBooks() ) ) )
						.invoke( a -> assertThat( a.getBooks() ).containsExactlyInAnyOrder( book1, book2 ) )
				) )
		);
	}

	@Test
	public void testFetchJoinQueryGetSingleResultOrNull(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( "select distinct a from Author a left join fetch a.books", Author.class )
						.getSingleResultOrNull()
						.invoke( a -> assertTrue( Hibernate.isInitialized( a.getBooks() ) ) )
						.invoke( a -> assertThat( a.getBooks() ).containsExactlyInAnyOrder( book1, book2 ) )
				) )
		);
	}

	@Test
	public void testFetchJoinQueryWithNull(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( "select a from Author a left join fetch a.books where 1=0", Author.class )
						.getSingleResultOrNull()
						.invoke( Assertions::assertNull )
				) )
		);
	}

	@Test
	public void testFetchAndRemove(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.getBooks() ) )
						// Remove one book from the fetched collection
						.invoke( books -> books.remove( book1 ) )
				) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( session -> session.find( Author.class, author.id )
								.chain( a -> session.fetch( a.getBooks() ) )
								.invoke( books -> assertThat( books ).containsExactly( book2 ) )
						)
				)
		);
	}

	@Test
	public void testFetchAndAdd(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		// Only book2 this time
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.getBooks() ) )
						.invoke( books -> assertThat( books ).containsExactly( book2 ) )
						.chain( books -> session.find( Book.class, book1.id ).invoke( books::add ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> assertFalse( Hibernate.isInitialized( a.getBooks() ) ) )
						.chain( a -> session.fetch( a.getBooks() ) )
						.invoke( books -> assertThat( books ).containsExactlyInAnyOrder( book1, book2 ) )
				) )
		);
	}

	@Test
	public void testSetNullAndFetch(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> a.books = null )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> assertThat( books ).isEmpty() )
				) )
		);
	}

	@Test
	public void testFetchAndClear(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.getBooks() ) )
						.invoke( List::clear )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> assertThat( books ).isEmpty() )
				) )
		);
	}


	@Entity(name = "Book")
	@Table(name = "OTMBook")
	static class Book {
		Book(String title) {
			this.title = title;
		}

		Book() {
		}

		@GeneratedValue
		@Id
		long id;

		@Basic(optional = false)
		String title;

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

	@Entity(name = "Author")
	@Table(name = "OTMAuthor")
	static class Author {
		Author(String name) {
			this.name = name;
		}

		public Author() {
		}

		@GeneratedValue
		@Id
		long id;

		@Basic(optional = false)
		String name;

		@OneToMany
		List<Book> books = new ArrayList<>();

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Book> getBooks() {
			return books;
		}

		public void setBooks(List<Book> books) {
			this.books = books;
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

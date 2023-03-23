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

import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

public class OneToManyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void test(TestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( book1, book2, author ) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> context.assertFalse( Hibernate.isInitialized( a.books ) ) )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> context.assertEquals( 2, books.size() ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( "select distinct a from Author a left join fetch a.books", Author.class )
						.getSingleResult()
						.invoke( a -> context.assertTrue( Hibernate.isInitialized( a.books ) ) )
						.invoke( a -> context.assertEquals( 2, a.books.size() ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( "select a from Author a left join fetch a.books", Author.class )
						.getSingleResultOrNull()
						.invoke( a -> context.assertTrue( Hibernate.isInitialized( a.books ) ) )
						.invoke( a -> context.assertEquals( 2, a.books.size() ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( "select a from Author a left join fetch a.books where 1=0", Author.class )
						.getSingleResultOrNull()
						.invoke( context::assertNull )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> books.remove( book1 ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> {
							context.assertEquals( 1, books.size() );
							context.assertEquals( book2.title, books.get( 0 ).title );
						} )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.chain( books -> session.find( Book.class, book1.id ).invoke( books::add ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> context.assertFalse( Hibernate.isInitialized( a.books ) ) )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> context.assertEquals( 2, books.size() ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> books.remove( book2 ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> {
							context.assertEquals( 1, books.size() );
							context.assertEquals( book1.title, books.get( 0 ).title );
						} )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.chain( books -> session.find( Book.class, book2.id ).invoke( books::add ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> context.assertFalse( Hibernate.isInitialized( a.books ) ) )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> context.assertEquals( 2, books.size() ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> books.add( books.remove( 0 ) ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> context.assertFalse( Hibernate.isInitialized( a.books ) ) )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> context.assertEquals( 2, books.size() ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.invoke( a -> a.books = null )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( Author.class, author.id )
						.chain( a -> session.fetch( a.books ) )
						.invoke( books -> context.assertTrue( books.isEmpty() ) )
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

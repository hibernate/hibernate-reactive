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
import jakarta.persistence.Basic;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.Table;

import org.hibernate.Hibernate;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import org.assertj.core.api.Assertions;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OrderedEmbeddableCollectionTest extends BaseReactiveTest {

	@RegisterExtension // This exposes a strange bug in the DB2 client
	public DBSelectionExtension dbRule = DBSelectionExtension.skipTestsFor( DB2 );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class );
	}

	@Test
	public void test(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );

		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session.persistAll( author ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.invoke( a -> assertFalse( Hibernate.isInitialized( a.books ) ) )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertEquals( 2, books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.createQuery(
																  "select distinct a from Author a left join fetch a.books",
																  Author.class
														  )
														  .getSingleResult()
														  .invoke( a -> assertTrue( Hibernate.isInitialized( a.books ) ) )
														  .invoke( a -> assertEquals( 2, a.books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> books.remove( 0 ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> {
											assertEquals( 1, books.size() );
											assertEquals( book2.title, books.get( 0 ).title );
										} )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> books.add( book1 ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.invoke( a -> assertFalse( Hibernate.isInitialized( a.books ) ) )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertEquals( 2, books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> books.remove( 1 ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> {
											assertEquals( 1, books.size() );
											assertEquals( book2.title, books.get( 0 ).title );
										} )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> books.add( book1 ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.invoke( a -> assertFalse( Hibernate.isInitialized( a.books ) ) )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertEquals( 2, books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> books.add( books.remove( 0 ) ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.invoke( a -> assertFalse( Hibernate.isInitialized( a.books ) ) )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertEquals( 2, books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.invoke( a -> a.books = null )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertTrue( books.isEmpty() ) )
								)
						)
		);
	}

	@Test
	public void testMultipleRemovesFromCollection(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Book book3 = new Book( "Third Book" );
		Book book4 = new Book( "Fourth Book" );
		Book book5 = new Book( "Fifth Book" );
		Author author = new Author( "Iain M Banks" );
		author.books.add( book1 );
		author.books.add( book2 );
		author.books.add( book3 );
		author.books.add( book4 );
		author.books.add( book5 );

		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session.persistAll( author ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.invoke( a -> assertFalse( Hibernate.isInitialized( a.books ) ) )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertEquals( 5, books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.createQuery(
																  "select distinct a from Author a left join fetch a.books",
																  Author.class
														  )
														  .getSingleResult()
														  .invoke( a -> assertTrue( Hibernate.isInitialized( a.books ) ) )
														  .invoke( a -> assertEquals( 5, a.books.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> {
											books.remove( 1 ); // Remove book2
											books.remove( 1 ); // Now, remove book3
										} )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Author.class, author.id )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> {
											assertEquals( 3, books.size() );
											assertEquals( book4.title, books.get( 1 ).title );
											Assertions.assertThat( books ).containsExactly( book1, book4, book5 );
										} )
								)
						)
		);
	}

	@Embeddable
	static class Book {
		Book(String title) {
			this.title = title;
		}

		Book() {
		}

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
	@Table(name = "ECAuthor")
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

		@ElementCollection
		@CollectionTable(name = "ECBook")
		@OrderColumn(name = "list_index")
		List<Book> books = new ArrayList<>();
	}
}

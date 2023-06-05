/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class ManyToManySetTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void test(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		book1.authors.add( author );
		book2.authors.add( author );
		author.books.add( book1 );
		author.books.add( book2 );

		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session.persistAll( book1, book2, author ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, transaction) -> session.find( Book.class, book1.id )
										.invoke( b -> assertFalse( Hibernate.isInitialized( b.authors ) ) )
										.chain( b -> session.fetch( b.authors ) )
										.invoke( authors -> assertEquals( 1, authors.size() ) )
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
								.withTransaction( (session, transaction) -> session.createQuery(
																  "select distinct a from Author a left join fetch a.books",
																  Author.class
														  )
														  .getSingleResult()
														  .invoke( a -> assertTrue( Hibernate.isInitialized( a.books ) ) )
														  .invoke( a -> assertEquals( 2, a.books.size() ) )
								)
						)
		);
	}

	@Entity(name = "Book")
	@Table(name = "MTMSBook")
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

		@ManyToMany
		Set<Author> authors = new HashSet<>();
	}

	@Entity(name = "Author")
	@Table(name = "MTMSAuthor")
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

		@ManyToMany(mappedBy = "authors")
		Set<Book> books = new HashSet<>();
	}
}

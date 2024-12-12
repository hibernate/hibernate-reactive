/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class ManyToManyMapTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void test(VertxTestContext context) {
		Book book1 = new Book( "Feersum Endjinn" );
		Book book2 = new Book( "Use of Weapons" );
		Author author = new Author( "Iain M Banks" );
		book1.authors.put( "a", author );
		book2.authors.put( "b", author );
		author.books.add( book1 );
		author.books.add( book2 );

		test(
				context,
				getMutinySessionFactory()
						.withTransaction( session -> session.persistAll( book1, book2, author ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session.find( Book.class, book1.id )
										.invoke( b -> assertFalse( Hibernate.isInitialized( b.authors ) ) )
										.chain( b -> session.fetch( b.authors ) )
										.invoke( authors -> assertEquals( 1, authors.size() ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session.find( Author.class, author.id )
										.invoke( a -> assertFalse( Hibernate.isInitialized( a.books ) ) )
										.chain( a -> session.fetch( a.books ) )
										.invoke( books -> assertThat( books ).containsExactlyInAnyOrder( book1, book2 ) )
								)
						)
						.chain( () -> getMutinySessionFactory().withTransaction( session -> session
								.createSelectionQuery( "select distinct a from Author a left join fetch a.books", Author.class )
								.getSingleResult()
								.invoke( a -> assertTrue( Hibernate.isInitialized( a.books ) ) )
								.invoke( a -> assertThat( a.books ).containsExactlyInAnyOrder( book1, book2 ) )
						) )
		);
	}

	@Entity(name = "Book")
	@Table(name = "MTMMBook")
	static class Book {

		Book() {
		}

		Book(String title) {
			this.title = title;
		}

		@Id
		@GeneratedValue
		long id;

		@Basic(optional = false)
		String title;

		@ManyToMany
		@MapKeyColumn(name = "mapkey")
		Map<String, Author> authors = new HashMap<>();

		@Override
		public boolean equals(Object object) {
			if ( object == null || getClass() != object.getClass() ) {
				return false;
			}
			Book book = (Book) object;
			return Objects.equals( title, book.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( title );
		}

		@Override
		public String toString() {
			return id + ":" + title;
		}
	}

	@Entity(name = "Author")
	@Table(name = "MTMMAuthor")
	static class Author {
		Author(String name) {
			this.name = name;
		}

		public Author() {
		}

		@Id
		@GeneratedValue
		long id;

		@Basic(optional = false)
		String name;

		@ManyToMany(mappedBy = "authors")
		Set<Book> books = new HashSet<>();

		@Override
		public boolean equals(Object object) {
			if ( object == null || getClass() != object.getClass() ) {
				return false;
			}
			Author author = (Author) object;
			return Objects.equals( name, author.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}
}

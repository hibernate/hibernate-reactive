/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.NamedNativeQuery;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ReactiveNamedNativeQueryMemento and ReactiveNamedSqmQueryMemento
 * to verify proper wrapping and delegation behavior.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class ReactiveNamedMementoTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Test
	public void testNamedHQLQueryReturnsReactiveImplementation(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> {
			Book book = new Book( 1L, "Reactive Hibernate", "Author1" );
			return session.persist( book )
					.chain( session::flush )
					.chain( () ->
						// Named query should return reactive query implementation
						session.createNamedQuery( "findAllBooks", Book.class )
								.getResultList()
					)
					.invoke( books ->
						assertThat( books )
								.extracting( Book::getTitle )
								.containsExactly( "Reactive Hibernate" )
					);
		} ) );
	}

	@Test
	public void testNamedNativeQueryReturnsReactiveImplementation(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> {
			Book book = new Book( 2L, "SQL Queries", "Author2" );
			return session.persist( book )
					.chain( session::flush )
					.chain( () ->
						// Named native query should return reactive query implementation
						session.createNamedQuery( "findAllBooksNative", Book.class )
								.getResultList()
					)
					.invoke( books ->
						assertThat( books )
								.extracting( Book::getTitle )
								.containsExactly( "SQL Queries" )
					);
		} ) );
	}

	@Test
	public void testNamedMutationQueryWorks(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> {
			Book book1 = new Book( 3L, "Book to Update", "Author3" );
			Book book2 = new Book( 4L, "Another Book", "Author4" );
			return session.persist( book1 )
					.chain( () -> session.persist( book2 ) )
					.chain( session::flush );
		} )
				.chain( () -> getMutinySessionFactory().withTransaction( session ->
					// Named mutation query should work reactively
					session.createNamedQuery( "updateBookTitle" )
							.setParameter( "newTitle", "Updated Title" )
							.setParameter( "id", 3L )
							.executeUpdate()
							.invoke( count -> assertThat( count ).isEqualTo( 1 ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( session ->
					session.find( Book.class, 3L )
							.invoke( book -> {
								assertThat( book ).isNotNull();
								assertThat( book.title ).isEqualTo( "Updated Title" );
							} )
				) )
		);
	}

	@Test
	public void testNamedSelectionQueryFromNativeMemento(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> {
			Book book = new Book( 5L, "Selection Test", "Author5" );
			return session.persist( book )
					.chain( session::flush )
					.chain( () ->
						// Test toSelectionQuery path for native memento
						session.createNamedQuery( "findBooksByAuthor", Book.class )
								.setParameter( 1, "Author5" )
								.getResultList()
					)
					.invoke( books ->
						assertThat( books )
								.extracting( Book::getTitle )
								.containsExactly( "Selection Test" )
					);
		} ) );
	}

	@Test
	public void testNamedQueryResultMapping(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> {
			Book book = new Book( 6L, "Mapping Test", "Author6" );
			return session.persist( book )
					.chain( session::flush )
					.chain( () ->
						// Test native query with result class mapping
						session.createNamedQuery( "findAllBooksNative", Book.class )
								.getResultList()
					)
					.invoke( books ->
						assertThat( books )
								.hasSize( 1 )
								.first()
								.satisfies( b -> {
									assertThat( b.title ).isEqualTo( "Mapping Test" );
									assertThat( b.author ).isEqualTo( "Author6" );
								} )
					);
		} ) );
	}

	@Entity(name = "MementoBook")
	@Table(name = "MEMENTO_BOOK")
	@NamedQuery(name = "findAllBooks", query = "select b from MementoBook b")
	@NamedQuery(name = "updateBookTitle", query = "update MementoBook b set b.title = :newTitle where b.id = :id")
	@NamedNativeQuery(
			name = "findAllBooksNative",
			query = "SELECT * FROM MEMENTO_BOOK",
			resultClass = Book.class
	)
	@NamedNativeQuery(
			name = "findBooksByAuthor",
			query = "SELECT * FROM MEMENTO_BOOK WHERE author = ?",
			resultClass = Book.class
	)
	public static class Book {
		@Id
		private Long id;
		private String title;
		private String author;

		public Book() {
		}

		public Book(Long id, String title, String author) {
			this.id = id;
			this.title = title;
			this.author = author;
		}

		public Long getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public String getAuthor() {
			return author;
		}

		public void setAuthor(String author) {
			this.author = author;
		}
	}
}

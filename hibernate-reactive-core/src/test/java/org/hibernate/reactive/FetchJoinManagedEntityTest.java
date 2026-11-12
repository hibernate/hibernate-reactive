/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.HashSet;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.hibernate.Hibernate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test fetch joins when the entity is already in the persistence context.
 * <p>
 * The query will find the entity already managed by the session,
 * and the initializer needs to resolve the sub-initializers for the
 * associations that are not initialized yet.
 * <p>
 * These tests cover the different ways of resolving the sub-initializers
 * of an entity that is already initialized. They don't depend on bytecode
 * enhancement: see {@code LazyBasicFieldTest} in the bytecode enhancement
 * integration tests for the case of a lazy basic attribute.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class FetchJoinManagedEntityTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	private final Author author = new Author( 1, "Iain M. Banks" );
	private final Book book1 = new Book( 1, "Consider Phlebas", author );
	private final Book book2 = new Book( 2, "The Player of Games", author );
	private final Book book3 = new Book( 3, "Use of Weapons", author );

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		author.getBooks().addAll( List.of( book1, book2, book3 ) );
		test( context, getMutinySessionFactory().withTransaction( s -> s.persistAll( author, book1, book2, book3 ) ) );
	}

	@Test
	public void fetchJoinManyToOneOfManagedEntity(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> s
				.find( Book.class, book1.getId() )
				.invoke( book -> assertThat( Hibernate.isInitialized( book.getAuthor() ) ).isFalse() )
				.chain( book -> s
						.createSelectionQuery( "from Book b join fetch b.author where b.id = :id", Book.class )
						.setParameter( "id", book1.getId() )
						.getSingleResult()
						.invoke( result -> {
							assertThat( result ).isSameAs( book );
							assertThat( Hibernate.isInitialized( result.getAuthor() ) ).isTrue();
							assertThat( result.getAuthor() ).isEqualTo( author );
						} )
				)
		) );
	}

	@Test
	public void fetchJoinOneToManyOfManagedEntity(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> s
				.find( Author.class, author.getId() )
				.invoke( found -> assertThat( Hibernate.isInitialized( found.getBooks() ) ).isFalse() )
				.chain( found -> s
						.createSelectionQuery( "from Author a join fetch a.books where a.id = :id", Author.class )
						.setParameter( "id", author.getId() )
						.getSingleResult()
						.invoke( result -> {
							assertThat( result ).isSameAs( found );
							assertThat( Hibernate.isInitialized( result.getBooks() ) ).isTrue();
							assertThat( result.getBooks() ).containsExactlyInAnyOrder( book1, book2, book3 );
						} )
				)
		) );
	}

	@Test
	public void nestedFetchJoinOfManagedEntities(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> s
				.find( Book.class, book1.getId() )
				.invoke( book -> assertThat( Hibernate.isInitialized( book.getAuthor() ) ).isFalse() )
				.chain( book -> s
						.createSelectionQuery(
								"from Book b join fetch b.author a join fetch a.books where b.id = :id",
								Book.class
						)
						.setParameter( "id", book1.getId() )
						.getSingleResult()
						.invoke( result -> {
							assertThat( result ).isSameAs( book );
							assertThat( Hibernate.isInitialized( result.getAuthor() ) ).isTrue();
							assertThat( result.getAuthor() ).isEqualTo( author );
							assertThat( Hibernate.isInitialized( result.getAuthor().getBooks() ) ).isTrue();
							assertThat( result.getAuthor().getBooks() ).containsExactlyInAnyOrder( book1, book2, book3 );
							assertThat( result.getAuthor().getBooks() ).contains( book );
						} )
				)
		) );
	}

	@Test
	public void fetchJoinWhenEntityIsInitializedByPreviousRow(VertxTestContext context) {
		// The author is the same in each row: after the first row,
		// the initializer of the author is the owner of the managed entity
		test( context, getMutinySessionFactory().withSession( s -> s
				.createSelectionQuery( "from Book b join fetch b.author a join fetch a.books order by b.id", Book.class )
				.getResultList()
				.invoke( books -> {
					assertThat( books ).containsExactly( book1, book2, book3 );
					final Author found = books.get( 0 ).getAuthor();
					assertThat( found ).isEqualTo( author );
					assertThat( books ).allSatisfy( book -> assertThat( book.getAuthor() ).isSameAs( found ) );
					assertThat( Hibernate.isInitialized( found.getBooks() ) ).isTrue();
					assertThat( found.getBooks() ).containsExactlyInAnyOrderElementsOf( books );
				} )
		) );
	}

	@Entity(name = "Author")
	@Table(name = "FJMEAuthor")
	public static class Author {
		@Id
		private Integer id;

		private String name;

		@OneToMany(mappedBy = "author")
		private Set<Book> books = new HashSet<>();

		public Author() {
		}

		public Author(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Set<Book> getBooks() {
			return books;
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof Author author
					&& Objects.equals( name, author.getName() );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}

	@Entity(name = "Book")
	@Table(name = "FJMEBook")
	public static class Book {
		@Id
		private Integer id;

		private String title;

		@ManyToOne(fetch = FetchType.LAZY)
		private Author author;

		public Book() {
		}

		public Book(Integer id, String title, Author author) {
			this.id = id;
			this.title = title;
			this.author = author;
		}

		public Integer getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public Author getAuthor() {
			return author;
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof Book book
					&& Objects.equals( title, book.getTitle() );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}

		@Override
		public String toString() {
			return id + ":" + title;
		}
	}
}

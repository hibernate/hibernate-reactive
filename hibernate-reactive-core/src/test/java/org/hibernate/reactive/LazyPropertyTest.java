/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.metamodel.Attribute;

import static jakarta.persistence.CascadeType.PERSIST;
import static jakarta.persistence.FetchType.LAZY;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lazy properties only work when the related bytecode enhancement is enabled.
 * We test bytecode enhancements in a separate module, new related tests should be created there.
 * I'm keeping this one because it seems to work and might highlight if something changes in the future.
 */
@Timeout(value = 10, timeUnit = MINUTES)

public class LazyPropertyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void testLazyProperty(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.getBooks().add( book1 );
		author2.getBooks().add( book2 );
		author2.getBooks().add( book3 );

		Attribute<? super Book, ?> Book_isbn = getSessionFactory().getMetamodel()
				.entity( Book.class ).getAttribute( "isbn" );

		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( author1, author2 ) )
				.thenCompose( v -> getSessionFactory()
						.withSession( session -> session
								.find( Book.class, book1.id )
								.thenCompose( book -> {
									assertFalse( Hibernate.isPropertyInitialized( book, "isbn" ) );
									return session.fetch( book, Book_isbn )
											.thenAccept( isbn -> {
												assertNotNull( isbn );
												assertTrue( Hibernate.isPropertyInitialized( book, "isbn" ) );
											} );
								} )
						)
				)
		);
	}

	@Entity(name = "Author")
	@Table(name = "authors")
	static class Author {
		@Id
		@GeneratedValue
		private Integer id;

		private String name;

		@OneToMany(mappedBy = "author", cascade = PERSIST)
		private List<Book> books = new ArrayList<>();

		public Author(String name) {
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

		public List<Book> getBooks() {
			return books;
		}

		public void setBooks(List<Book> books) {
			this.books = books;
		}
	}

	@Entity(name = "Book")
	@Table(name = "books")
	static class Book extends LazyAttributeLoadingInterceptor
			implements PersistentAttributeInterceptable {
		@Id
		@GeneratedValue
		private Integer id;

		@Basic(fetch = LAZY)
		private String isbn;

		private String getIsbn() {
			return isbn;
		}

		private String title;

		@ManyToOne(fetch = LAZY)
		private Author author;

		public Book(String isbn, String title, Author author) {
			super( new EntityRelatedState( "Book", singleton( "isbn" ) ), 1, null );
			this.title = title;
			this.isbn = isbn;
			this.author = author;
		}

		public Book() {
			super( new EntityRelatedState( "Book", singleton( "isbn" ) ), 1, null );
		}

		@Override
		public PersistentAttributeInterceptor $$_hibernate_getInterceptor() {
			return this;
		}

		@Override
		public void $$_hibernate_setInterceptor(PersistentAttributeInterceptor interceptor) {
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public void setIsbn(String isbn) {
			this.isbn = isbn;
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
	}
}

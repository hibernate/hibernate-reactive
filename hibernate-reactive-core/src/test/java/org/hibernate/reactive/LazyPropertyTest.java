/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.junit.Test;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.metamodel.Attribute;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singleton;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.FetchType.LAZY;

public class LazyPropertyTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@Test
	public void testLazyProperty(TestContext context) {

		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		Attribute<? super Book, ?> Book_isbn = getSessionFactory().getMetamodel()
				.entity(Book.class).getAttribute("isbn");

		test( context,
				getSessionFactory().withTransaction(
						//persist the Authors with their Books in a transaction
						(session, tx) -> session.persist(author1, author2)
				).thenCompose (
						v -> getSessionFactory().withSession(
								//retrieve a Book
								session -> session.find(Book.class, book1.id)
										//print its title
										.thenCompose( book -> {
											context.assertFalse( Hibernate.isPropertyInitialized(book, "isbn") );
											return session.fetch( book, Book_isbn )
													.thenAccept( isbn -> {
														context.assertNotNull( isbn );
														context.assertTrue( Hibernate.isPropertyInitialized(book, "isbn") );
													} );
										} )
						)
				)
		);
	}


	@Entity(name="Author")
	@Table(name="authors")
	static class Author {
		@Id @GeneratedValue
		Integer id;

		String name;

		@OneToMany(mappedBy = "author", cascade = PERSIST)
		List<Book> books = new ArrayList<>();

		Author(String name) {
			this.name = name;
		}

		Author() {}
	}

	@Entity(name="Book")
	@Table(name="books")
	static class Book extends LazyAttributeLoadingInterceptor
			implements PersistentAttributeInterceptable {
		@Id
		@GeneratedValue
		Integer id;

		@Basic(fetch = LAZY)
		String isbn;

		public String getIsbn() {
			return isbn;
		}

		String title;

		@ManyToOne(fetch = LAZY)
		Author author;

		Book(String isbn, String title, Author author) {
			super("Book", 1, singleton("isbn"), null);
			this.title = title;
			this.isbn = isbn;
			this.author = author;
		}

		Book() {
			super("Book", 1, singleton("isbn"), null);
		}

		@Override
		public PersistentAttributeInterceptor $$_hibernate_getInterceptor() {
			return this;
		}
		@Override
		public void $$_hibernate_setInterceptor(PersistentAttributeInterceptor interceptor) {}
	}
}

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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout( value = 5, timeUnit = TimeUnit.MINUTES )
public class EagerOneToManyAssociationTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class, Book.class );
	}

	@Test
	public void findBookWithAuthors(VertxTestContext context) {
		final Book goodOmens = new Book( 7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21426321, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2132511, "Terry Pratchett", goodOmens );
		goodOmens.getAuthors().add( neilGaiman );
		goodOmens.getAuthors().add( terryPratchett );

		test(
				context,
				openSession()
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist(goodOmens) )
								.thenCompose( v -> s.persist(neilGaiman) )
								.thenCompose( v -> s.persist(terryPratchett) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, goodOmens.getId() ) )
						.thenAccept( optionalBook -> {
							assertNotNull( optionalBook );
							assertEquals( 2, optionalBook.getAuthors().size() );
							assertTrue( optionalBook.getAuthors().contains( neilGaiman )  );
							assertTrue( optionalBook.getAuthors().contains( terryPratchett )  );
						} )
		);
	}

	@Test
	public void getBookWithAuthors(VertxTestContext context) {
		final Book goodOmens = new Book( 7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21426321, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2132511, "Terry Pratchett", goodOmens );
		goodOmens.getAuthors().add( neilGaiman );
		goodOmens.getAuthors().add( terryPratchett );

		test( context, getSessionFactory()
				.withStatelessSession( session -> session
						.insert( goodOmens, neilGaiman, terryPratchett ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( session -> session
						.get( Book.class, goodOmens.getId() )
						.thenAccept( optionalBook -> {
							assertNotNull( optionalBook );
							assertEquals( 2, optionalBook.getAuthors().size() );
							assertTrue( optionalBook.getAuthors().contains( neilGaiman ) );
							assertTrue( optionalBook.getAuthors().contains( terryPratchett ) );
						} )
				) )
		);
	}

	@Entity(name = "Book")
	@Table(name = Book.TABLE)
	public static class Book {
		public static final String TABLE = "Book1";

		@Id
		private Integer id;
		private String title;

		@OneToMany(fetch = FetchType.EAGER, mappedBy="book")
		private List<Author> authors = new ArrayList<>();

		public Book() {
		}

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

		public List<Author> getAuthors() {
			return authors;
		}

		public void setAuthors(List<Author> authors) {
			this.authors = authors;
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

	@Entity(name = "Author")
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author1";

		@Id
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.EAGER)
		private Book book;

		public Author() {
		}

		public Author(Integer id, String name, Book book) {
			this.id = id;
			this.name = name;
			this.book = book;
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

		public Book getBook() {
			return book;
		}

		public void setBook(Book book) {
			this.book = book;
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

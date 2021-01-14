/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;

import org.junit.Before;
import org.junit.Test;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class EagerOneToManyAssociationTest extends BaseReactiveTest {

	private Book goodOmens;
	private Author neilGaiman;
	private Author terryPratchett;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		goodOmens = new Book( 7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		neilGaiman = new Author( 21426321, "Neil Gaiman", goodOmens );
		terryPratchett = new Author( 2132511, "Terry Pratchett", goodOmens );
		goodOmens.getAuthors().add( neilGaiman );
		goodOmens.getAuthors().add( terryPratchett );

		test(
				context,
				completedFuture( openSession() )
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist(goodOmens) )
								.thenCompose( v -> s.persist(neilGaiman) )
								.thenCompose( v -> s.persist(terryPratchett) )
								.thenCompose( v -> s.flush() )
						)
		);
	}

	@Test
	public void findBookWithAuthors(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Book.class, goodOmens.getId())
						.thenAccept( foundBook -> {
							context.assertNotNull( foundBook );
							context.assertEquals( 2, foundBook.getAuthors().size() );
							context.assertTrue( foundBook.getAuthors().contains( neilGaiman )  );
							context.assertTrue( foundBook.getAuthors().contains( terryPratchett )  );
						} )
		);
	}

	// @Test
	// FIXME: This test does not yet work. Edits for OneToMany Associations with Lists of Embedded entity types
	// does not work
	public void clearBookAuthors(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Book.class, goodOmens.getId() )
						.thenCompose( foundBook -> {
							foundBook.getAuthors().clear();
							return session.flush();
						} )
						.thenCompose( s -> openSession().find( Book.class, goodOmens.getId() )
								.thenAccept( changedBook -> {
									context.assertEquals( 0, changedBook.getAuthors().size() );
								} )
						)
		);
	}

	// @Test
	// FIXME: This test does not yet work. Edits for OneToMany Associations with Lists of Embedded entity types
	// does not work
	public void setNewAuthorsCollection(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Book.class, goodOmens.getId())
						.thenCompose( foundBook -> {
								context.assertNotNull( foundBook );
								context.assertEquals( 2, foundBook.getAuthors().size() );
								Author peterPiper = new Author( 2111111, "Peter Piper", goodOmens );
								goodOmens.getAuthors().clear();
								goodOmens.getAuthors().add( peterPiper );
											  return session.flush();
						  }
						)
						.thenCompose( s -> openSession().find( Book.class, goodOmens.getId()) )
						.thenAccept( changedBook -> {
							context.assertNotNull( changedBook );
							context.assertEquals( 1, changedBook.getAuthors().size() );
							Author firstAuthor = changedBook.getAuthors().get(0);
							context.assertEquals( "Peter Piper",  firstAuthor.getName());
						} )
		);
	}

	@Entity
	@Table(name = Book.TABLE)
	public static class Book {
		public static final String TABLE = "Book";

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

	@Entity
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author";

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

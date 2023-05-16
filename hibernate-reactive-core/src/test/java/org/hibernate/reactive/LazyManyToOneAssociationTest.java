/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.FetchProfile;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.NamedAttributeNode;
import jakarta.persistence.NamedEntityGraph;
import jakarta.persistence.Table;

import static org.hibernate.Hibernate.isInitialized;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LazyManyToOneAssociationTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void fetchProfileWithOneAuthor(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 5, "Charlie Mackesy", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.enableFetchProfile( "withBook" )
										.find( Author.class, author.getId() ) ) )
						.thenAccept( optionalAuthor -> {
							assertNotNull( optionalAuthor );
							assertEquals( author, optionalAuthor );
							assertTrue( isInitialized( optionalAuthor.getBook() ) );
							assertEquals( book, optionalAuthor.getBook() );
						} )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.find( Book.class, book.getId() ) ) )
						.thenAccept( optionalBook -> {
							assertNotNull( optionalBook );
							assertEquals( book, optionalBook );
						} )
		);
	}

	@Test
	public void namedEntityGraphWithOneAuthor(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 5, "Charlie Mackesy", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.find(
										s.getEntityGraph( Author.class, "withBook" ),
										author.getId()
								) ) )
						.thenAccept( optionalAuthor -> {
							assertNotNull( optionalAuthor );
							assertEquals( author, optionalAuthor );
							assertTrue( isInitialized( optionalAuthor.getBook() ) );
							assertEquals( book, optionalAuthor.getBook() );
						} )
						.thenCompose( v -> openSession().thenCompose( s -> s.find( Book.class, book.getId() ) ) )
						.thenAccept( optionalBook -> {
							assertNotNull( optionalBook );
							assertEquals( book, optionalBook );
						} )
		);
	}

	@Test
	public void newEntityGraphWithOneAuthor(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 5, "Charlie Mackesy", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession().thenCompose( s -> {
							EntityGraph<Author> graph = s.createEntityGraph( Author.class );
							graph.addAttributeNodes( "book" );
							return s.find( graph, author.getId() )
									.thenAccept( optionalAuthor -> {
										assertNotNull( optionalAuthor );
										assertEquals( author, optionalAuthor );
										assertTrue( isInitialized( optionalAuthor.getBook() ) );
										assertEquals( book, optionalAuthor.getBook() );
									} );
						} ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.find( Book.class, book.getId() ) ) )
						.thenAccept( optionalBook -> {
							assertNotNull( optionalBook );
							assertEquals( book, optionalBook );
						} )
		);
	}

	@Test
	public void fetchWithOneAuthor(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 5, "Charlie Mackesy", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.find( Author.class, author.getId() )
										.thenCompose( optionalAuthor -> {
											assertNotNull( optionalAuthor );
											assertEquals( author, optionalAuthor );
											assertFalse( isInitialized( optionalAuthor.getBook() ) );
											return s.fetch( optionalAuthor.getBook() ).thenAccept(
													fetchedBook -> {
														assertNotNull( fetchedBook );
														assertEquals( book, fetchedBook );
														assertTrue( isInitialized( optionalAuthor.getBook() ) );
													} );
										} )
								) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.find( Book.class, book.getId() ) ) )
						.thenAccept( optionalBook -> {
							assertNotNull( optionalBook );
							assertEquals( book, optionalBook );
						} )
		);
	}

	@Test
	public void fetchWithTwoAuthors(VertxTestContext context) {
		final Book goodOmens = new Book( 72433, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21421, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2111, "Terry Pratchett", goodOmens );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( goodOmens )
								.thenCompose( v -> s.persist( terryPratchett ) )
								.thenCompose( v -> s.persist( neilGaiman ) )
								.thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> s.find( Author.class, neilGaiman.getId() )
										.thenCompose( optionalAuthor -> {
											assertNotNull( optionalAuthor );
											assertEquals( neilGaiman, optionalAuthor );
											assertFalse( isInitialized( optionalAuthor.getBook() ) );
											return s.fetch( optionalAuthor.getBook() ).thenAccept(
													fetchedBook -> {
														assertNotNull( fetchedBook );
														assertEquals( goodOmens, fetchedBook );
														assertTrue( isInitialized( optionalAuthor.getBook() ) );
													} );
										} )
								) )
		);
	}

	@Test
	public void fetchWithTwoAuthorsStateless(VertxTestContext context) {
		final Book goodOmens = new Book( 72433, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21421, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2111, "Terry Pratchett", goodOmens );

		test(
				context,
				getSessionFactory().openStatelessSession()
						.thenCompose( s -> s.insert( goodOmens )
								.thenCompose( v -> s.insert( terryPratchett ) )
								.thenCompose( v -> s.insert( neilGaiman ) )
								.thenCompose( v -> s.close() ) )
						.thenCompose( v -> getSessionFactory().openStatelessSession()
								.thenCompose( s -> s.get( Author.class, neilGaiman.getId() )
										.thenCompose( optionalAuthor -> {
											assertNotNull( optionalAuthor );
											assertEquals( neilGaiman, optionalAuthor );
											assertFalse( isInitialized( optionalAuthor.getBook() ) );
											return s.fetch( optionalAuthor.getBook() ).thenAccept(
													fetchedBook -> {
														assertNotNull( fetchedBook );
														assertEquals( goodOmens, fetchedBook );
														assertTrue( isInitialized( optionalAuthor.getBook() ) );
													} );
										} )
										.thenCompose( vv -> s.close() )
								) )
		);
	}

	@Test
	public void manyToOneIsNull(VertxTestContext context) {
		final Author author = new Author( 5, "Charlie Mackesy", null );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( author ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Author.class, author.getId() ) )
						.thenAccept( optionalAuthor -> {
							assertNotNull( optionalAuthor );
							assertEquals( author, optionalAuthor );
							assertNull( author.book, "Book must be null" );
						} )
		);
	}

	@Entity(name = "Book")
	@Table(name = Book.TABLE)
	@DiscriminatorValue("N")
	public static class Book {
		public static final String TABLE = "Book3";

		@Id
		private Integer id;
		private String title;

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

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( !( o instanceof Book ) ) {
				return false;
			}
			Book book = (Book) o;
			return Objects.equals( getTitle(), book.getTitle() );
		}

		@Override
		public int hashCode() {
			return Objects.hash( getTitle() );
		}
	}

	@FetchProfile(name = "withBook",
			fetchOverrides = @FetchProfile.FetchOverride(
					entity = Author.class, association = "book",
					mode = FetchMode.JOIN))

	@NamedEntityGraph(name = "withBook",
			attributeNodes = @NamedAttributeNode("book")
	)

	@Entity(name = "Author")
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author3";

		@Id
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.LAZY)
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
			if ( !( o instanceof Author ) ) {
				return false;
			}
			Author author = (Author) o;
			return Objects.equals( getName(), author.getName() );
		}

		@Override
		public int hashCode() {
			return Objects.hash( getName() );
		}
	}
}

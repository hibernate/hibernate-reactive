/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.SoftDelete;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import org.hibernate.reactive.util.impl.CompletionStages;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests @SoftDelete annotation applied to collection relationships.
 */
public class SoftDeleteCollectionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class, Book.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> {
					Author author1 = new Author( "J.R.R. Tolkien" );
					Author author2 = new Author( "Stephen King" );

					Book book1 = new Book( "The Hobbit", author1 );
					Book book2 = new Book( "The Lord of the Rings", author1 );
					Book book3 = new Book( "The Silmarillion", author1 );
					Book book4 = new Book( "The Shining", author2 );
					Book book5 = new Book( "It", author2 );

					author1.addBook( book1 );
					author1.addBook( book2 );
					author1.addBook( book3 );
					author2.addBook( book4 );
					author2.addBook( book5 );

					return session.persistAll( author1, author2, book1, book2, book3, book4, book5 );
				} )
		);
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		return getSessionFactory()
				.withTransaction( s -> s.createNativeQuery( "delete from Book" ).executeUpdate()
						.thenCompose( v -> s.createNativeQuery( "delete from Author" ).executeUpdate() )
						.thenCompose( CompletionStages::voidFuture ) );
	}

	@Test
	public void testCollectionFiltersSoftDeletedEntities(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Initially, all books should be in the collections
				.withSession( s -> s
						.createSelectionQuery( "from Author a join fetch a.books where a.name = :name", Author.class )
						.setParameter( "name", "J.R.R. Tolkien" )
						.getSingleResult()
						.invoke( author -> assertThat( author.getBooks() ).hasSize( 3 ) )
				)
				// Soft delete one book
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.createMutationQuery( "delete from Book where title = :title" )
						.setParameter( "title", "The Silmarillion" )
						.executeUpdate()
				) )
				// The author's collection should now have only 2 books
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Author a join fetch a.books where a.name = :name", Author.class )
						.setParameter( "name", "J.R.R. Tolkien" )
						.getSingleResult()
						.invoke( author -> {
							assertThat( author.getBooks() ).hasSize( 2 );
							assertThat( author.getBooks() )
									.extracting( Book::getTitle )
									.containsExactlyInAnyOrder( "The Hobbit", "The Lord of the Rings" );
						} )
				) )
		);
	}

	@Test
	public void testMultipleSoftDeletes(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete two books from the first author
				.withTransaction( s -> s
						.createMutationQuery( "delete from Book where title in (:titles)" )
						.setParameter( "titles", List.of( "The Hobbit", "The Silmarillion" ) )
						.executeUpdate()
				)
				// Verify the collection is updated
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Author a join fetch a.books where a.name = :name", Author.class )
						.setParameter( "name", "J.R.R. Tolkien" )
						.getSingleResult()
						.invoke( author -> {
							assertThat( author.getBooks() ).hasSize( 1 );
							assertThat( author.getBooks().get( 0 ).getTitle() ).isEqualTo( "The Lord of the Rings" );
						} )
				) )
				// Other author's books should remain intact
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Author a join fetch a.books where a.name = :name", Author.class )
						.setParameter( "name", "Stephen King" )
						.getSingleResult()
						.invoke( author -> assertThat( author.getBooks() ).hasSize( 2 ) )
				) )
		);
	}

	@Test
	public void testEntityRemoveSoftDeletes(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete a book using HQL
				.withTransaction( s -> s
						.createMutationQuery( "delete from Book where title = :title" )
						.setParameter( "title", "The Shining" )
						.executeUpdate()
				)
				// Verify it's removed from the collection
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Author a join fetch a.books where a.name = :name", Author.class )
						.setParameter( "name", "Stephen King" )
						.getSingleResult()
						.invoke( author -> {
							assertThat( author.getBooks() ).hasSize( 1 );
							assertThat( author.getBooks().get( 0 ).getTitle() ).isEqualTo( "It" );
						} )
				) )
		);
	}

	@Test
	public void testDirectQueryStillFindsNonDeleted(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete one book
				.withTransaction( s -> s
						.createMutationQuery( "delete from Book where title = :title" )
						.setParameter( "title", "The Hobbit" )
						.executeUpdate()
				)
				// Direct query should only find non-deleted books
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Book order by id", Book.class )
						.getResultList()
						.invoke( books -> assertThat( books ).hasSize( 4 ) )
				) )
		);
	}

	@Entity(name = "Author")
	@Table(name = "Author")
	public static class Author {
		@Id
		@GeneratedValue
		private Integer id;

		private String name;

		@OneToMany(mappedBy = "author")
		private List<Book> books = new ArrayList<>();

		public Author() {
		}

		public Author(String name) {
			this.name = name;
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

		public void addBook(Book book) {
			books.add( book );
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

	@Entity(name = "Book")
	@Table(name = "Book")
	@SoftDelete
	public static class Book {
		@Id
		@GeneratedValue
		private Integer id;

		private String title;

		@ManyToOne
		private Author author;

		public Book() {
		}

		public Book(String title, Author author) {
			this.title = title;
			this.author = author;
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

		public Author getAuthor() {
			return author;
		}

		public void setAuthor(Author author) {
			this.author = author;
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

		@Override
		public String toString() {
			return "Book{id=" + id + ", title='" + title + "'}";
		}
	}
}

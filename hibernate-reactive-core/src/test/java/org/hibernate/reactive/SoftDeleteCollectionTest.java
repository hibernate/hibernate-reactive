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
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import org.hibernate.reactive.util.impl.CompletionStages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Tests @SoftDelete annotation applied to collection relationships.
 * Covers basic filtering, CascadeType.REMOVE, and orphanRemoval=true scenarios.
 */
public class SoftDeleteCollectionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(
				Author.class, Book.class,
				CascadeAuthor.class, CascadeBook.class,
				OrphanAuthor.class, OrphanBook.class
		);
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

	// Entities are annotated with @SoftDelete, we need to execute a native query to actually empty the table
	@Override
	protected CompletionStage<Void> cleanDb() {
		return getSessionFactory()
				.withTransaction( s -> s.createNativeQuery( "delete from Book" ).executeUpdate()
						.thenCompose( v -> s.createNativeQuery( "delete from Author" ).executeUpdate() )
						.thenCompose( v -> s.createNativeQuery( "delete from CascadeBook" ).executeUpdate() )
						.thenCompose( v -> s.createNativeQuery( "delete from CascadeAuthor" ).executeUpdate() )
						.thenCompose( v -> s.createNativeQuery( "delete from OrphanBook" ).executeUpdate() )
						.thenCompose( v -> s.createNativeQuery( "delete from OrphanAuthor" ).executeUpdate() )
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
				// Verify the soft-deleted book still exists in the table with deleted=true
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from Book order by id" )
						.getResultList()
						.invoke( rows -> {
							// All 5 rows must still exist (soft delete does not remove rows)
							assertThat( rows ).hasSize( 5 );
							long deletedCount = rows.stream()
									.map( r -> (Object[]) r )
									.filter( r -> dbType() == DB2
											? (short) r[2] == (short) 1
											: (boolean) r[2] )
									.count();
							assertThat( deletedCount ).isEqualTo( 1 );
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
				// All 5 rows must still exist; exactly 2 should be marked deleted
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from Book order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 5 );
							long deletedCount = rows.stream()
									.map( r -> (Object[]) r )
									.filter( r -> dbType() == DB2
											? (short) r[2] == (short) 1
											: (boolean) r[2] )
									.count();
							assertThat( deletedCount ).isEqualTo( 2 );
						} )
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
				// Verify the soft-deleted book still exists in the table
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from Book order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 5 );
							long deletedCount = rows.stream()
									.map( r -> (Object[]) r )
									.filter( r -> dbType() == DB2
											? (short) r[2] == (short) 1
											: (boolean) r[2] )
									.count();
							assertThat( deletedCount ).isEqualTo( 1 );
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
				// Direct HQL query should only find non-deleted books
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Book order by id", Book.class )
						.getResultList()
						.invoke( books -> assertThat( books ).hasSize( 4 ) )
				) )
				// Native query must show all 5 rows still present in the table
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from Book order by id" )
						.getResultList()
						.invoke( rows -> assertThat( rows ).hasSize( 5 ) )
				) )
		);
	}

	@Test
	public void testCascadeRemoveSoftDeletesBooks(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Create a CascadeAuthor with two books
				.withTransaction( session -> {
					CascadeAuthor author = new CascadeAuthor( "Cascade Author" );
					CascadeBook book1 = new CascadeBook( "Cascade Book 1", author );
					CascadeBook book2 = new CascadeBook( "Cascade Book 2", author );
					author.addBook( book1 );
					author.addBook( book2 );
					return session.persistAll( author, book1, book2 );
				} )
				// Remove the author — CascadeType.REMOVE should propagate to books
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "from CascadeAuthor", CascadeAuthor.class )
						.getSingleResult()
						.chain( s::remove )
				) )
				// Books should no longer be visible via HQL (soft-delete filter applied)
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from CascadeBook", CascadeBook.class )
						.getResultList()
						.invoke( books -> assertThat( books ).isEmpty() )
				) )
				// But rows must still exist in the table with deleted=true
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from CascadeBook order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							for ( Object row : rows ) {
								Object[] r = (Object[]) row;
								if ( dbType() == DB2 ) {
									assertThat( (short) r[2] ).isEqualTo( (short) 1 );
								}
								else {
									assertThat( (boolean) r[2] ).isTrue();
								}
							}
						} )
				) )
		);
	}

	@Test
	public void testOrphanRemovalSoftDeletesBook(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Create an OrphanAuthor with two books
				.withTransaction( session -> {
					OrphanAuthor author = new OrphanAuthor( "Orphan Author" );
					OrphanBook book1 = new OrphanBook( "Orphan Book 1", author );
					OrphanBook book2 = new OrphanBook( "Orphan Book 2", author );
					author.addBook( book1 );
					author.addBook( book2 );
					return session.persistAll( author, book1, book2 );
				} )
				// Remove one book from the collection — orphanRemoval should soft-delete it
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery(
								"from OrphanAuthor a join fetch a.books order by a.id",
								OrphanAuthor.class
						)
						.getSingleResult()
						.invoke( author -> author.getBooks().remove( 0 ) )
				) )
				// HQL should return only 1 book
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from OrphanBook", OrphanBook.class )
						.getResultList()
						.invoke( books -> assertThat( books ).hasSize( 1 ) )
				) )
				// Both rows must still exist in the table; one should be marked deleted
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from OrphanBook order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							long deletedCount = rows.stream()
									.map( r -> (Object[]) r )
									.filter( r -> dbType() == DB2
											? (short) r[2] == (short) 1
											: (boolean) r[2] )
									.count();
							assertThat( deletedCount ).isEqualTo( 1 );
						} )
				) )
		);
	}

	// -------------------------------------------------------------------------
	// Entities for basic collection tests
	// -------------------------------------------------------------------------

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

	// -------------------------------------------------------------------------
	// Entities for CascadeType.REMOVE test
	// Both author and book have @SoftDelete to avoid FK constraint violations.
	// -------------------------------------------------------------------------

	@Entity(name = "CascadeAuthor")
	@Table(name = "CascadeAuthor")
	@SoftDelete
	public static class CascadeAuthor {
		@Id
		@GeneratedValue
		private Integer id;

		private String name;

		@OneToMany(mappedBy = "author", cascade = CascadeType.REMOVE)
		private List<CascadeBook> books = new ArrayList<>();

		public CascadeAuthor() {
		}

		public CascadeAuthor(String name) {
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public List<CascadeBook> getBooks() {
			return books;
		}

		public void addBook(CascadeBook book) {
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
			CascadeAuthor that = (CascadeAuthor) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}

	@Entity(name = "CascadeBook")
	@Table(name = "CascadeBook")
	@SoftDelete
	public static class CascadeBook {
		@Id
		@GeneratedValue
		private Integer id;

		private String title;

		@ManyToOne
		private CascadeAuthor author;

		public CascadeBook() {
		}

		public CascadeBook(String title, CascadeAuthor author) {
			this.title = title;
			this.author = author;
		}

		public Integer getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public CascadeAuthor getAuthor() {
			return author;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			CascadeBook that = (CascadeBook) o;
			return Objects.equals( title, that.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( title );
		}

		@Override
		public String toString() {
			return "CascadeBook{id=" + id + ", title='" + title + "'}";
		}
	}

	// -------------------------------------------------------------------------
	// Entities for orphanRemoval=true test
	// -------------------------------------------------------------------------

	@Entity(name = "OrphanAuthor")
	@Table(name = "OrphanAuthor")
	public static class OrphanAuthor {
		@Id
		@GeneratedValue
		private Integer id;

		private String name;

		@OneToMany(mappedBy = "author", orphanRemoval = true)
		private List<OrphanBook> books = new ArrayList<>();

		public OrphanAuthor() {
		}

		public OrphanAuthor(String name) {
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public List<OrphanBook> getBooks() {
			return books;
		}

		public void addBook(OrphanBook book) {
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
			OrphanAuthor that = (OrphanAuthor) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}

	@Entity(name = "OrphanBook")
	@Table(name = "OrphanBook")
	@SoftDelete
	public static class OrphanBook {
		@Id
		@GeneratedValue
		private Integer id;

		private String title;

		@ManyToOne
		private OrphanAuthor author;

		public OrphanBook() {
		}

		public OrphanBook(String title, OrphanAuthor author) {
			this.title = title;
			this.author = author;
		}

		public Integer getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public OrphanAuthor getAuthor() {
			return author;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			OrphanBook that = (OrphanBook) o;
			return Objects.equals( title, that.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( title );
		}

		@Override
		public String toString() {
			return "OrphanBook{id=" + id + ", title='" + title + "'}";
		}
	}
}

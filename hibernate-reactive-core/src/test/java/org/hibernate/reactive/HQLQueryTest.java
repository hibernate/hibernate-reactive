/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static jakarta.persistence.CascadeType.PERSIST;
import static jakarta.persistence.FetchType.LAZY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class HQLQueryTest extends BaseReactiveTest {

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	Author author1 = new Author( "Madeline Miller");
	Author author2 = new Author( "Andrea Camilleri");
	Book book1 = new Book( "9780316556347", "Circe", author1);
	Book book2 = new Book( "0-330-49286-1 ", "The Shape of Water", author2);
	Book book3 = new Book( "978-0-14-311203-7", "The Patience of the Spider", author2);

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class, Book.class, Author.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persistAll( spelt, rye, almond, author1, author2, book1, book2, book3 ) ) );
	}

	@Test
	public void testAutoFlushOnSingleResult(VertxTestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withTransaction( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createSelectionQuery( "from Flour where id = " + semolina.getId(), Flour.class ).getSingleResult() )
				.thenAccept( found -> assertEquals( semolina, found ) ) )
		);
	}

	@Test
	public void testAutoFlushOnResultList(VertxTestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withTransaction( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createSelectionQuery( "from Flour order by name", Flour.class ).getResultList() )
				.thenAccept( results -> assertThat( results ).containsExactly( almond, rye, semolina, spelt ) )
		) );
	}

	@Test
	public void testSelectScalarString(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			Stage.SelectionQuery<String> qr = s.createSelectionQuery( "SELECT 'Prova' FROM Flour WHERE id = " + rye.getId(), String.class );
			assertNotNull( qr );
			return qr.getSingleResult();
		} ).thenAccept( found -> assertEquals( "Prova", found ) ) );
	}

	@Test
	public void testSelectScalarCount(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			Stage.SelectionQuery<Long> qr = s.createSelectionQuery( "SELECT count(*) FROM Flour", Long.class );
			assertNotNull( qr );
			return qr.getSingleResult();
		} ).thenAccept( found -> assertEquals( 3L, found ) ) );
	}

	@Test
	public void testSelectWithMultipleScalarValues(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.SelectionQuery<?> qr = s.createSelectionQuery( "SELECT 'Prova', f.id FROM Flour f WHERE f.id = " + rye.getId(), Object[].class );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( found -> {
				  assertTrue( found instanceof Object[] );
				  assertEquals( "Prova", ( (Object[]) found )[0] );
				  assertEquals( rye.getId(), ( (Object[]) found )[1] );
			  } )
		);
	}

	@Test
	public void testSingleResultQueryOnId(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.SelectionQuery<?> qr = s.createSelectionQuery( "FROM Flour WHERE id = 1", Flour.class );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> assertEquals( spelt, flour ) )
		);
	}

	@Test
	public void testSingleResultQueryOnName(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.SelectionQuery<?> qr = s.createSelectionQuery( "FROM Flour WHERE name = 'Almond'", Flour.class );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testFromQuery(VertxTestContext context) {
		test( context, getSessionFactory()
				.withSession( s -> {
					Stage.SelectionQuery<Flour> qr = s.createSelectionQuery( "FROM Flour ORDER BY name", Flour.class );
					assertNotNull( qr );
					return qr.getResultList();
				} )
				.thenAccept( results -> assertThat( results ).containsExactly( almond, rye, spelt ) )
		);
	}

	@Test
	public void testSelectNewConstructor(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.createQuery(
										"SELECT NEW Book(b.title, b.author) FROM Book b ORDER BY b.title DESC",
										Book.class
								  ).getResultList()
				)
				.invoke( books -> {
					assertThat( books ).hasSize( 3 );
					books.forEach( book -> {
						String name = book.getAuthor().getName();
						if ( book.getTitle().equals( "Circe" ) ) {
							assertThat( name ).isEqualTo( "Madeline Miller" );
						}
						else {
							assertThat( name ).isEqualTo( "Andrea Camilleri" );
						}
					} );
				} )
		);
	}

	@Entity(name = "Flour")
	@Table(name = "Flour")
	public static class Flour {
		@Id
		private Integer id;
		private String name;
		private String description;
		private String type;

		public Flour() {
		}

		public Flour(Integer id, String name, String description, String type) {
			this.id = id;
			this.name = name;
			this.description = description;
			this.type = type;
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

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Flour flour = (Flour) o;
			return Objects.equals( name, flour.name ) &&
					Objects.equals( description, flour.description ) &&
					Objects.equals( type, flour.type );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, description, type );
		}
	}

	@Entity(name = "Book")
	@Table(name = "Book_HQL")
	public static class Book {
		@Id
		@GeneratedValue
		private Integer id;

		private String isbn;

		private String title;

		@ManyToOne(fetch = LAZY)
		private Author author;

		public Book() {
		}

		public Book(String title, Author author) {
			this.title = title;
			this.author = author;
		}

		public Book(String isbn, String title, Author author) {
			this.isbn = isbn;
			this.title = title;
			this.author = author;
			author.books.add( this );
		}

		public Integer getId() {
			return id;
		}

		public String getIsbn() {
			return isbn;
		}

		public String getTitle() {
			return title;
		}

		public Author getAuthor() {
			return author;
		}
	}

	@Entity(name = "Author")
	@Table(name = "Author_HQL")
	public static class Author {
		@Id @GeneratedValue
		private Integer id;

		private String name;

		@OneToMany(mappedBy = "author", cascade = PERSIST)
		private List<Book> books = new ArrayList<>();

		public Author() {
		}

		public Author(String name) {
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public List<Book> getBooks() {
			return books;
		}
	}
}

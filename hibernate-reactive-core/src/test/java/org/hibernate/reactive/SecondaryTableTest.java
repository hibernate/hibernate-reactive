/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;


import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SecondaryTable;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

@DisabledFor( value = DB2, reason = "IllegalStateException: Needed to have 6 in buffer but only had 0" )
public class SecondaryTableTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class, Book.class );
	}

	@Test
	public void testRootClassViaAssociation(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date(), false );
		final Author author = new Author( "Charlie Mackesy", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2.find( Author.class, author.getId() ) )
						.thenAccept( auth -> {
							assertThat( auth ).isNotNull();
							assertThat( auth ).isEqualTo( author );
							assertThat( auth.getBook().getTitle() ).isEqualTo( book.getTitle() );
							assertThat( book.isForbidden() ).isFalse();
						} )
		);
	}

	@Test
	public void testSubclassViaAssociation(VertxTestContext context) {
		final Book book = new Book( 6, "Necronomicon", new Date(), true );
		final Author author = new Author( "Abdul Alhazred", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
								.thenCompose( v -> s.find( Author.class, author.getId() ) )
						)
						.thenAccept( auth -> {
							assertThat( auth ).isNotNull();
							assertThat( auth ).isEqualTo( author );
							assertThat( auth.getBook().getTitle() ).isEqualTo( book.getTitle() );
							assertThat( book.isForbidden() ).isTrue();
						} )
		);
	}

	@Test
	public void testRootClassViaFind(VertxTestContext context) {

		final Book novel = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date(), false );
		final Author author = new Author( "Charlie Mackesy", novel );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( novel )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, 6 ) )
						.thenAccept( book -> {
							assertThat( book ).isNotNull();
							assertThat( book.isForbidden() ).isFalse();
							assertThat( book.getTitle() ).isEqualTo( "The Boy, The Mole, The Fox and The Horse" );
						} )
		);
	}

	@Test
	public void testSubclassViaFind(VertxTestContext context) {
		final Book spells = new Book( 6, "Necronomicon", new Date(), true );
		final Author author = new Author( "Abdul Alhazred", spells );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( spells )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, 6 ) )
						.thenAccept( book -> {
							assertThat( book ).isNotNull();
							assertThat( book.isForbidden() ).isTrue();
							assertThat( book.getTitle() ).isEqualTo( "Necronomicon" );
						} )
		);
	}

	@Entity(name = "Book")
	@Table(name = Book.TABLE1)
	@SecondaryTable(name = Book.TABLE2)
	@SecondaryTable(name = Book.TABLE3)
	public static class Book {
		public static final String TABLE1 = "Book";
		public static final String TABLE2 = "SpellBook";
		public static final String TABLE3 = "Extra";

		@Id
		private Integer id;
		private String title;

		@Temporal(TemporalType.DATE)
		@Column(table = Book.TABLE2)
		private Date published;
		@Column(table = Book.TABLE2)
		private boolean forbidden;

		public Book() {
		}

		public Book(Integer id, String title, Date published, boolean forbidden) {
			this.id = id;
			this.title = title;
			this.published = published;
			this.forbidden = forbidden;
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

		public Date getPublished() {
			return published;
		}

		public void setPublished(Date published) {
			this.published = published;
		}

		public boolean isForbidden() {
			return forbidden;
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

		public static final String TABLE = "Author";

		@Id
		@GeneratedValue
		private Integer id;
		private String name;

		@ManyToOne
		private Book book;

		public Author() {
		}

		public Author(String name, Book book) {
			this.name = name;
			this.book = book;
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

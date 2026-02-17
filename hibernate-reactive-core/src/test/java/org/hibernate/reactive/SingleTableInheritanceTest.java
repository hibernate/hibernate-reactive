/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class SingleTableInheritanceTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class, Book.class, SpellBook.class );
	}

	@Test
	public void testMultiLoad(VertxTestContext context) {
		final Book book1 = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date() );
		final SpellBook book2 = new SpellBook( 3, "Necronomicon", true, new Date() );
		final Book book3 = new Book( 2, "Hibernate in Action", new Date() );

		test(
				context,
				openSession()
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist( book1 ) )
								.thenCompose( v -> s.persist( book2 ) )
								.thenCompose( v -> s.persist( book3 ) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, book3.getId(), book1.getId(), book2.getId() ) )
						.thenAccept( list -> assertThat( list )
							.hasSize( 3 )
							.extracting( Book::getTitle )
							.containsExactly( book3.getTitle(), book1.getTitle(), book2.getTitle() )
						)
		);
	}

	@Test
	public void testRootClassViaAssociation(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date() );
		final Author author = new Author( "Charlie Mackesy", book );

		test( context, openSession()
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
				} )
		);
	}

	@Test
	public void testSubclassViaAssociation(VertxTestContext context) {
		final SpellBook book = new SpellBook( 6, "Necronomicon", true, new Date() );
		final Author author = new Author( "Abdul Alhazred", book );

		test( context, openSession()
				.thenCompose( s -> s.persist( book )
						.thenCompose( v -> s.persist( author ) )
						.thenCompose( v -> s.flush() )
						.thenCompose( v -> s.find( Author.class, author.getId() ) )
				)
				.thenAccept( auth -> {
					assertThat( auth ).isNotNull();
					assertThat( auth ).isEqualTo( author );
					assertThat( auth.getBook().getTitle() ).isEqualTo( book.getTitle() );
				} )
		);
	}

	@Test
	public void testRootClassViaFind(VertxTestContext context) {

		final Book novel = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date() );
		final Author author = new Author( "Charlie Mackesy", novel );

		test( context, openSession()
				.thenCompose( s -> s.persist( novel )
						.thenCompose( v -> s.persist( author ) )
						.thenCompose( v -> s.flush() )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Book.class, 6 ) )
				.thenAccept( book -> {
					assertThat( book ).isNotNull();
					assertThat( book ).isNotInstanceOf( SpellBook.class );
					assertThat( book.getTitle() ).isEqualTo( "The Boy, The Mole, The Fox and The Horse" );
				} )
		);
	}

	@Test
	public void testSubclassViaFind(VertxTestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
		final Author author = new Author( "Abdul Alhazred", spells );

		test( context, openSession()
				.thenCompose( s -> s.persist( spells )
						.thenCompose( v -> s.persist( author ) )
						.thenCompose( v -> s.flush() )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Book.class, 6 ) )
				.thenAccept( book -> {
					assertThat( book ).isNotNull();
					assertThat( book ).isInstanceOf( SpellBook.class );
					assertThat( book.getTitle() ).isEqualTo( "Necronomicon" );
				} )
		);
	}

	@Test
	public void testQueryUpdate(VertxTestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );

		test( context, openSession()
				.thenCompose( s -> s.persist( spells ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s
						.createMutationQuery( "update Book set title=title||' II' where title='Necronomicon'" )
						.executeUpdate() )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Book.class, 6 ) )
				.thenAccept( book -> {
					assertThat( book ).isNotNull();
					assertThat( book ).isInstanceOf( SpellBook.class );
					assertThat( book.getTitle() ).isEqualTo( "Necronomicon II" );
				} )
				.thenCompose( v -> openSession() ).thenCompose( s -> s
						.createMutationQuery( "delete Book where title='Necronomicon II'" )
						.executeUpdate() )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Book.class, 6 ) )
				.thenAccept( Assertions::assertNull )
		);
	}

	@Test
	public void testQueryUpdateWithParameters(VertxTestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );

		test( context, openSession()
				.thenCompose( s -> s.persist( spells ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createMutationQuery( "update Book set title=title||:sfx where title=:tit" )
						.setParameter( "sfx", " II" )
						.setParameter( "tit", "Necronomicon" )
						.executeUpdate() )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Book.class, 6 ) )
				.thenAccept( book -> {
					assertThat( book ).isNotNull();
					assertThat( book ).isInstanceOf( SpellBook.class );
					assertThat( book.getTitle() ).isEqualTo( "Necronomicon II" );
				} )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createMutationQuery( "delete Book where title=:tit" )
						.setParameter( "tit", "Necronomicon II" )
						.executeUpdate() )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Book.class, 6 ) )
				.thenAccept( Assertions::assertNull )
		);
	}

	@Entity(name = "SpellBook")
	@DiscriminatorValue("S")
	public static class SpellBook extends Book {

		private boolean forbidden;

		public SpellBook(Integer id, String title, boolean forbidden, Date published) {
			super( id, title, published );
			this.forbidden = forbidden;
		}

		SpellBook() {
		}

		public boolean getForbidden() {
			return forbidden;
		}
	}

	@Entity(name = "Book")
	@Table(name = Book.TABLE)
	@DiscriminatorValue("N")
	public static class Book {

		public static final String TABLE = "BookSTIT";

		@Id
		private Integer id;
		private String title;
		@Temporal(TemporalType.DATE)
		private Date published;

		public Book() {
		}

		public Book(Integer id, String title, Date published) {
			this.id = id;
			this.title = title;
			this.published = published;
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

		public static final String TABLE = "AuthorSTIT";

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

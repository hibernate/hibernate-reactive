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
import jakarta.persistence.Column;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class UnionSubclassInheritanceTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class, SpellBook.class );
	}

	@Test
	public void testRootClassViaAssociation(VertxTestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
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
						} )
		);
	}

	@Test
	public void testSubclassViaAssociation(VertxTestContext context) {
		final SpellBook book = new SpellBook( 6, "Necronomicon", true, new Date());
		final Author author = new Author( "Abdul Alhazred", book );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
								.thenCompose( v -> s.find( Author.class, author.getId() ) )
								.thenAccept( auth -> {
									assertThat( auth ).isNotNull();
									assertThat( auth ).isEqualTo( author );
									assertThat( auth.getBook().getTitle() ).isEqualTo( book.getTitle() );
								} )
						)
		);
	}

	@Test
	public void testRootClassViaFind(VertxTestContext context) {

		final Book novel = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
		final Author author = new Author( "Charlie Mackesy", novel );

		test( context,
				openSession()
						.thenCompose(s -> s.persist(novel)
								.thenCompose(v -> s.persist(author))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							assertThat( book ).isNotNull();
							assertThat( book ).isNotInstanceOf( SpellBook.class );
							assertThat( book.getTitle() ).isEqualTo( "The Boy, The Mole, The Fox and The Horse" );
						}));
	}

	@Test
	public void testSubclassViaFind(VertxTestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				openSession()
						.thenCompose(s -> s.persist(spells)
								.thenCompose(v -> s.persist(author))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							assertThat( book ).isNotNull();
							assertThat( book ).isInstanceOf( SpellBook.class );
							assertThat( book.getTitle() ).isEqualTo( "Necronomicon" );
						}));
	}

	@Test
	public void testQueryUpdate(VertxTestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
//		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				openSession()
						.thenCompose( s -> s.persist(spells).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("update SpellBook set title='x' where forbidden=false").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("update SpellBook set forbidden=false where title='Necronomicon'").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("update Book set title=title||' II' where title='Necronomicon'").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find(Book.class, 6))
						.thenAccept( book -> {
							assertThat( book ).isNotNull();
							assertThat( book ).isInstanceOf( SpellBook.class );
							assertThat( book.getTitle() ).isEqualTo( "Necronomicon II" );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("delete Book where title='Necronomicon II'").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept( Assertions::assertNull )
		);
	}

	@Test
	public void testQueryUpdateWithParameters(VertxTestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
//		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				openSession()
						.thenCompose( s -> s.persist(spells).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("update SpellBook set forbidden=:fob where title=:tit")
								.setParameter("fob", false)
								.setParameter("tit", "Necronomicon")
								.executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("update Book set title=title||:sfx where title=:tit")
								.setParameter("sfx", " II")
								.setParameter("tit", "Necronomicon")
								.executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find(Book.class, 6))
						.thenAccept( book -> {
							assertThat( book ).isNotNull();
							assertThat( book ).isInstanceOf( SpellBook.class );
							assertThat( book.getTitle() ).isEqualTo( "Necronomicon II" );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createMutationQuery("delete Book where title=:tit")
								.setParameter("tit", "Necronomicon II")
								.executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept( Assertions::assertNull )
		);
	}

	@Entity(name="SpellBook")
	@Table(name = "SpellBookUS")
	@DiscriminatorValue("S")
	public static class SpellBook extends Book {

		private boolean forbidden;

		public SpellBook(Integer id, String title, boolean forbidden, Date published) {
			super(id, title, published);
			this.forbidden = forbidden;
		}

		SpellBook() {}

		public boolean getForbidden() {
			return forbidden;
		}
	}

	@Entity(name="Book")
	@Table(name = "BookUS")
	@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
	public static class Book {

		@Id private Integer id;
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
	@Table(name = "AuthorUS")
	public static class Author {

		@Id @GeneratedValue
		private Integer id;

		@Column(name = "`name`")
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

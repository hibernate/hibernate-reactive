/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import javax.persistence.*;
import java.util.Date;
import java.util.Objects;

public class UnionSubclassInheritanceTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( SpellBook.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Book", "Author", "SpellBook" ) );
	}

	@Test
	public void testRootClassViaAssociation(TestContext context) {
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
							context.assertNotNull( auth );
							context.assertEquals( author, auth );
							context.assertEquals( book.getTitle(), auth.getBook().getTitle()  );
						} )
		);
	}

	@Test
	public void testSubclassViaAssociation(TestContext context) {
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
									context.assertNotNull( auth );
									context.assertEquals( author, auth );
									context.assertEquals( book.getTitle(), auth.getBook().getTitle()  );
								} )
						)
		);
	}

	@Test
	public void testRootClassViaFind(TestContext context) {

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
							context.assertNotNull(book);
							context.assertFalse(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "The Boy, The Mole, The Fox and The Horse");
						}));
	}

	@Test
	public void testSubclassViaFind(TestContext context) {
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
							context.assertNotNull(book);
							context.assertTrue(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "Necronomicon");
						}));
	}

	@Test
	public void testQueryUpdate(TestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
//		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				openSession()
						.thenCompose( s -> s.persist(spells).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("update SpellBook set title='x' where forbidden=false").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("update SpellBook set forbidden=false where title='Necronomicon'").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("update Book set title=title||' II' where title='Necronomicon'").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find(Book.class, 6))
						.thenAccept( book -> {
							context.assertNotNull(book);
							context.assertTrue(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "Necronomicon II");
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("delete Book where title='Necronomicon II'").executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept( context::assertNull )
		);
	}

	@Test
	public void testQueryUpdateWithParameters(TestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
//		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				openSession()
						.thenCompose( s -> s.persist(spells).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("update SpellBook set forbidden=:fob where title=:tit")
								.setParameter("fob", false)
								.setParameter("tit", "Necronomicon")
								.executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("update Book set title=title||:sfx where title=:tit")
								.setParameter("sfx", " II")
								.setParameter("tit", "Necronomicon")
								.executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find(Book.class, 6))
						.thenAccept( book -> {
							context.assertNotNull(book);
							context.assertTrue(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "Necronomicon II");
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.withTransaction( t -> s.createQuery("delete Book where title=:tit")
								.setParameter("tit", "Necronomicon II")
								.executeUpdate() ) )
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept( context::assertNull )
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

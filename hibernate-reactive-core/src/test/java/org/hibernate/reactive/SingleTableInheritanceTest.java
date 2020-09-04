/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.*;
import java.util.Date;
import java.util.Objects;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class SingleTableInheritanceTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SingleTableInheritanceTest.Book.class );
		configuration.addAnnotatedClass( SingleTableInheritanceTest.SpellBook.class );
		configuration.addAnnotatedClass( SingleTableInheritanceTest.Author.class );
		return configuration;
	}

	@Test
	public void testMultiLoad(TestContext context) {
		final Book book1 = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
		final SpellBook book2 = new SpellBook( 3, "Necronomicon", true, new Date());
		final Book book3 = new Book( 2, "Hibernate in Action", new Date());

		test(
				context,
				completedFuture( openSession() )
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist( book1 ) )
								.thenCompose( v -> s.persist( book2 ) )
								.thenCompose( v -> s.persist( book3 ) )
								.thenCompose( v -> s.flush() )
						)
						.thenApply( v -> openSession())
						.thenCompose( s -> s.find(Book.class, book3.getId(), book1.getId(), book2.getId()) )
						.thenAccept( list -> {
							context.assertEquals(3, list.size());
							context.assertEquals( book3.getTitle(), list.get(0).getTitle());
							context.assertEquals( book1.getTitle(), list.get(1).getTitle());
							context.assertEquals( book2.getTitle(), list.get(2).getTitle());
						})
		);
	}

	@Test
	public void testRootClassViaAssociation(TestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
		final Author author = new Author( "Charlie Mackesy", book );

		test(
				context,
				completedFuture( openSession() )
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
						)
						.thenApply( v -> openSession())
						.thenCompose( s2 -> s2.find( Author.class, author.getId() ) )
						.thenAccept( auth -> {
							context.assertNotNull( auth );
							context.assertEquals( author, auth );
							context.assertEquals( book.getTitle(), auth.getBook().getTitle() );
						} )
		);
	}

	@Test
	public void testSubclassViaAssociation(TestContext context) {
		final SpellBook book = new SpellBook( 6, "Necronomicon", true, new Date());
		final Author author = new Author( "Abdul Alhazred", book );

		test(
				context,
				completedFuture( openSession() )
						.thenCompose( s -> s.persist( book )
								.thenCompose( v -> s.persist( author ) )
								.thenCompose( v -> s.flush() )
								.thenCompose( v -> s.find( Author.class, author.getId() ) )
						)
						.thenAccept( auth -> {
							context.assertNotNull( auth );
							context.assertEquals( author, auth );
							context.assertEquals( book.getTitle(), auth.getBook().getTitle()  );
						} )
		);
	}

	@Test
	public void testRootClassViaFind(TestContext context) {

		final Book novel = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
		final Author author = new Author( "Charlie Mackesy", novel );

		test( context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(novel)
								.thenCompose(v -> s.persist(author))
								.thenCompose(v -> s.flush())
						)
						.thenApply( v -> openSession())
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							context.assertNotNull(book);
							context.assertFalse(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "The Boy, The Mole, The Fox and The Horse");
						}));
	}

	@Test
	public void testSubclassViaFind(TestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date());
		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(spells)
								.thenCompose(v -> s.persist(author))
								.thenCompose(v -> s.flush())
						)
						.thenApply( v -> openSession())
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
				completedFuture( openSession() )
						.thenCompose( s -> s.persist(spells).thenCompose( v -> s.flush() ) )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.createQuery("update Book set title=title||' II' where title='Necronomicon'").executeUpdate() )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find(Book.class, 6))
						.thenAccept( book -> {
							context.assertNotNull(book);
							context.assertTrue(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "Necronomicon II");
						} )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.createQuery("delete Book where title='Necronomicon II'").executeUpdate() )
						.thenApply( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept( book -> context.assertNull(book) )
		);
	}

	@Test
	public void testQueryUpdateWithParameters(TestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date() );
//		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				completedFuture( openSession() )
						.thenCompose( s -> s.persist(spells).thenCompose( v -> s.flush() ) )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.createQuery("update Book set title=title||:sfx where title=:tit")
								.setParameter("sfx", " II")
								.setParameter("tit", "Necronomicon")
								.executeUpdate() )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find(Book.class, 6))
						.thenAccept( book -> {
							context.assertNotNull(book);
							context.assertTrue(book instanceof SpellBook);
							context.assertEquals(book.getTitle(), "Necronomicon II");
						} )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.createQuery("delete Book where title=:tit")
								.setParameter("tit", "Necronomicon II")
								.executeUpdate() )
						.thenApply( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept( book -> context.assertNull(book) )
		);
	}

	@Entity(name = "SpellBook")
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

	@Entity(name = "Book")
	@Table(name = "BookST")
	@DiscriminatorValue("N")
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

	@Entity
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author";

		@Id @GeneratedValue
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

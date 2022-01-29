/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.FetchProfile;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.EntityGraph;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LazyOneToManyAssociationWithFetchTest extends BaseReactiveTest {

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Writer", "Tome" ) );
	}

	@Test
	public void findBookWithFetchAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(goodOmens)
								.thenCompose(v -> s.persist(neilGaiman))
								.thenCompose(v -> s.persist(terryPratchett))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Book.class, goodOmens.getId())
								.thenCompose(
										book -> s.fetch(book.getAuthors())
								))
						.thenAccept(optionalAssociation -> {
							context.assertTrue( Hibernate.isInitialized(optionalAssociation) );
							context.assertNotNull(optionalAssociation);
							context.assertTrue(optionalAssociation.contains(neilGaiman));
							context.assertTrue(optionalAssociation.contains(terryPratchett));
						})
		);

	}

	@Test
	public void findBookWithStaticFetchAuthors(TestContext context) {
		final Book goodOmens = new Book( 7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21426321, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2132511, "Terry Pratchett", goodOmens );
		goodOmens.getAuthors().add( neilGaiman );
		goodOmens.getAuthors().add( terryPratchett );
		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens)
								.thenCompose( v -> s.persist(neilGaiman) )
								.thenCompose( v -> s.persist(terryPratchett) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, goodOmens.getId() )
								.thenCompose(
										book -> Stage.fetch( book.getAuthors())
								) )
						.thenAccept( optionalAssociation -> {
							context.assertTrue( Hibernate.isInitialized(optionalAssociation) );
							context.assertNotNull(optionalAssociation);
							context.assertTrue(optionalAssociation.contains(neilGaiman));
							context.assertTrue(optionalAssociation.contains(terryPratchett));
						} )
		);
	}

	@Test
	public void findBookWithNamedEntityGraphFetchAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(goodOmens)
								.thenCompose(v -> s.persist(neilGaiman))
								.thenCompose(v -> s.persist(terryPratchett))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( s.getEntityGraph(Book.class, "withAuthors"), goodOmens.getId() ) )
						.thenAccept(book -> {
							context.assertTrue( Hibernate.isInitialized(book.authors) );
							List<Author> optionalAssociation = book.authors;
							context.assertNotNull(optionalAssociation);
							context.assertTrue(optionalAssociation.contains(neilGaiman));
							context.assertTrue(optionalAssociation.contains(terryPratchett));
						})
		);

	}

	@Test
	public void findBookWithNewEntityGraphFetchAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(goodOmens)
								.thenCompose(v -> s.persist(neilGaiman))
								.thenCompose(v -> s.persist(terryPratchett))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> {
							EntityGraph<Book> graph = s.createEntityGraph(Book.class);
							graph.addAttributeNodes("authors");
							return s.find( graph, goodOmens.getId() );
						})
						.thenAccept(book -> {
							context.assertTrue( Hibernate.isInitialized(book.authors) );
							List<Author> optionalAssociation = book.authors;
							context.assertNotNull(optionalAssociation);
							context.assertTrue(optionalAssociation.contains(neilGaiman));
							context.assertTrue(optionalAssociation.contains(terryPratchett));
						})
		);

	}

	@Test
	public void queryBookWithNamedEntityGraphFetchAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(goodOmens)
								.thenCompose(v -> s.persist(neilGaiman))
								.thenCompose(v -> s.persist(terryPratchett))
								.thenCompose(v -> s.flush()))
						.thenCompose(v -> openSession())
						.thenCompose( s -> s.createQuery( "from Tome b where b.id=?1", Book.class)
								.setPlan( s.getEntityGraph(Book.class, "withAuthors") )
								.setParameter(1, goodOmens.getId() )
								.getSingleResult() )
						.thenAccept(book -> {
							context.assertTrue( Hibernate.isInitialized(book.authors) );
							List<Author> optionalAssociation = book.authors;
							context.assertNotNull(optionalAssociation);
							context.assertTrue(optionalAssociation.contains(neilGaiman));
							context.assertTrue(optionalAssociation.contains(terryPratchett));
						})
						.thenCompose(v -> openSession())
						.thenCompose( s -> {
							EntityGraph<Author> graph = s.createEntityGraph(Author.class);
							graph.addAttributeNodes("book");
							return s.createQuery("from Writer w where w.id=?1", Author.class)
									.setPlan(graph)
									.setParameter(1, neilGaiman.getId())
									.getSingleResult();
						})
						.thenAccept(author -> context.assertTrue( Hibernate.isInitialized(author.book) ) )
		);

	}

	@Test
	public void findBookWithFetchProfileAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(goodOmens)
								.thenCompose(v -> s.persist(neilGaiman))
								.thenCompose(v -> s.persist(terryPratchett))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.enableFetchProfile("withAuthors").find(Book.class, goodOmens.getId() ) )
						.thenAccept(book -> {
							context.assertTrue( Hibernate.isInitialized(book.authors) );
							List<Author> optionalAssociation = book.authors;
							context.assertNotNull(optionalAssociation);
							context.assertTrue(optionalAssociation.contains(neilGaiman));
							context.assertTrue(optionalAssociation.contains(terryPratchett));
						})
		);

	}

	@Test
	public void getBookWithFetchAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test( context, getSessionFactory()
				.withStatelessSession( session -> session
					.insert( goodOmens, neilGaiman, terryPratchett ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( session -> session
						.get( Book.class, goodOmens.getId() ).thenCompose( book -> session
								.fetch( book.getAuthors() )
						) )
						.thenAccept( optionalAssociation -> {
							context.assertTrue( Hibernate.isInitialized( optionalAssociation ) );
							context.assertNotNull( optionalAssociation );
							context.assertTrue( optionalAssociation.contains( neilGaiman ) );
							context.assertTrue( optionalAssociation.contains( terryPratchett ) );
						} )
				)
		);
	}

	@Test
	public void getBookWithEntityGraphAuthors(TestContext context) {
		final Book goodOmens = new Book(7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch");
		final Author neilGaiman = new Author(21426321, "Neil Gaiman", goodOmens);
		final Author terryPratchett = new Author(2132511, "Terry Pratchett", goodOmens);
		goodOmens.getAuthors().add(neilGaiman);
		goodOmens.getAuthors().add(terryPratchett);

		test( context, getSessionFactory()
				.withStatelessSession( session -> session
						.insert( goodOmens, neilGaiman, terryPratchett ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( session -> {
					EntityGraph<Book> graph = session.createEntityGraph( Book.class );
					graph.addAttributeNodes( "authors" );
					return session.get( graph, goodOmens.getId() );
				} ) )
				.thenAccept( book -> {
					List<Author> optionalAssociation = book.authors;
					context.assertTrue( Hibernate.isInitialized( optionalAssociation ) );
					context.assertNotNull( optionalAssociation );
					context.assertTrue( optionalAssociation.contains( neilGaiman ) );
					context.assertTrue( optionalAssociation.contains( terryPratchett ) );
				} )
		);
	}

	@FetchProfile(name = "withAuthors",
			fetchOverrides = @FetchProfile.FetchOverride(
					entity = Book.class, association = "authors",
					mode = FetchMode.JOIN))

	@NamedEntityGraph(name="withAuthors",
			attributeNodes = @NamedAttributeNode("authors")
	)

	@Entity(name =  "Tome")
	@Table(name = Book.TABLE)
	public static class Book {
		public static final String TABLE = "Book4";

		@Id
		private Integer id;
		private String title;

		@OneToMany(fetch = FetchType.LAZY, mappedBy="book")
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

	@Entity(name = "Writer")
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author4";

		@Id
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.LAZY, optional = false)
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

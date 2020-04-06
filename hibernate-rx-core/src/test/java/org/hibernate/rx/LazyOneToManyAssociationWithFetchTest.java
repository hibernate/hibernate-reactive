package org.hibernate.rx;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LazyOneToManyAssociationWithFetchTest extends BaseRxTest {

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( Author.class );
		return configuration;
	}

	@Test
	public void findBookWithAuthors(TestContext context) {
		final Book goodOmens = new Book( 7242353, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21426321, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2132511, "Terry Pratchett", goodOmens );
		goodOmens.getAuthors().add( neilGaiman );
		goodOmens.getAuthors().add( terryPratchett );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(goodOmens) )
						.thenCompose( s -> s.persist(neilGaiman) )
						.thenCompose( s -> s.persist(terryPratchett) )
						.thenCompose( s -> s.flush() )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Book.class, goodOmens.getId() )
								.thenCompose(
										book -> s.fetch( book.get().getAuthors())
								) )
						.thenAccept( optionalAssociation -> {
							context.assertTrue(optionalAssociation.isPresent());
							context.assertTrue(optionalAssociation.get().contains(neilGaiman));
							context.assertTrue(optionalAssociation.get().contains(terryPratchett));

						} )
		);
	}

	@Entity
	@Table(name = Book.TABLE)
	public static class Book {
		public static final String TABLE = "Book";

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

	@Entity
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author";

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

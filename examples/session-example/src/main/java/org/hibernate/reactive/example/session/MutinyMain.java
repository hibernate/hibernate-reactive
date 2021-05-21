package org.hibernate.reactive.example.session;

import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;

import java.time.LocalDate;

import static java.lang.System.out;
import static java.time.Month.JANUARY;
import static java.time.Month.JUNE;
import static java.time.Month.MAY;
import static javax.persistence.Persistence.createEntityManagerFactory;
import static org.hibernate.reactive.mutiny.Mutiny.SessionFactory;
import static org.hibernate.reactive.mutiny.Mutiny.fetch;

/**
 * Demonstrates the use of Hibernate Reactive with the
 * {@link io.smallrye.mutiny.Uni Mutiny}-based API.
 */
public class MutinyMain {

	// The first argument can be used to select a persistenceUnit.
	// Check resources/META-INF/persistence.xml for available names.
	public static void main(String[] args) {
		out.println( "== Mutiny API Example ==" );

		// obtain a factory for reactive sessions based on the
		// standard JPA configuration properties specified in
		// resources/META-INF/persistence.xml
		SessionFactory factory =
				createEntityManagerFactory( persistenceUnitName( args ) )
						.unwrap(SessionFactory.class);

		// define some test data
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1, LocalDate.of(1994, JANUARY, 1));
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2, LocalDate.of(1999, MAY, 1));
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2, LocalDate.of(1992, JUNE, 1));
		author1.getBooks().add(book1);
		author2.getBooks().add(book2);
		author2.getBooks().add(book3);

		try {
			// obtain a reactive session
			factory.withTransaction(
					// persist the Authors with their Books in a transaction
					(session, tx) -> session.persistAll( author1, author2 )
			)
					// wait for it to finish
					.await().indefinitely();

			factory.withSession(
					// retrieve a Book
					session -> session.find( Book.class, book1.getId() )
							// print its title
							.invoke( book -> out.println( book.getTitle() + " is a great book!" ) )
			)
					.await().indefinitely();

			factory.withSession(
					// retrieve both Authors at once
					session -> session.find( Author.class, author1.getId(), author2.getId() )
							.invoke( authors -> authors.forEach( author -> out.println( author.getName() ) ) )
			)
					.await().indefinitely();

			factory.withSession(
					// retrieve an Author
					session -> session.find( Author.class, author2.getId() )
							// lazily fetch their books
							.chain( author -> fetch( author.getBooks() )
									// print some info
									.invoke( books -> {
										out.println( author.getName() + " wrote " + books.size() + " books" );
										books.forEach( book -> out.println( book.getTitle() ) );
									} )
							)
			)
					.await().indefinitely();

			factory.withSession(
					// query the Book titles
					session -> session.createQuery(
							"select title, author.name from Book order by title desc",
							Object[].class
					)
							.getResultList()
							.invoke( rows -> rows.forEach(
									row -> out.printf( "%s (%s)\n", row[0], row[1] )
							) )
			)
					.await().indefinitely();

			factory.withSession(
					// query the entire Book entities
					session -> session.createQuery(
							"from Book book join fetch book.author order by book.title desc",
							Book.class
					)
							.getResultList()
							.invoke( books -> books.forEach(
									b -> out.printf(
											"%s: %s (%s)\n",
											b.getIsbn(),
											b.getTitle(),
											b.getAuthor().getName()
									)
							) )
			)
					.await().indefinitely();

			factory.withSession(
					// use a criteria query
					session -> {
						CriteriaQuery<Book> query = factory.getCriteriaBuilder().createQuery( Book.class );
						Root<Author> a = query.from( Author.class );
						Join<Author, Book> b = a.join( Author_.books );
						query.where( a.get( Author_.name ).in( "Neal Stephenson", "William Gibson" ) );
						query.select( b );
						return session.createQuery( query ).getResultList().invoke(
								books -> books.forEach( book -> out.println( book.getTitle() ) )
						);
					}
			)
					.await().indefinitely();

			factory.withSession(
					// retrieve a Book
					session -> session.find( Book.class, book1.getId() )
							// fetch a lazy field of the Book
							.chain( book -> session.fetch( book, Book_.published )
									// print the lazy field
									.invoke( published -> out.printf(
											"'%s' was published in %d\n",
											book.getTitle(),
											published.getYear()
									) )
							)
			)
					.await().indefinitely();

			factory.withTransaction(
					// retrieve a Book
					(session, tx) -> session.find( Book.class, book2.getId() )
							// delete the Book
							.chain( session::remove )
			)
					.await().indefinitely();

			factory.withTransaction(
					// delete all the Books in a transaction
					(session, tx) -> session.createQuery( "delete Book" ).executeUpdate()
							//delete all the Authors
							.call( () -> session.createQuery( "delete Author" ).executeUpdate() )
			)
					.await().indefinitely();

		}
		finally {
			// remember to shut down the factory
			factory.close();
		}
	}

	/**
	 * Return the persistence unit name to use in the example.
	 *
	 * @param args the first element is the persistence unit name if present
	 * @return the selected persistence unit name or the default one
	 */
	public static String persistenceUnitName(String[] args) {
		return args.length > 0 ? args[0] : "postgresql-example";
	}
}

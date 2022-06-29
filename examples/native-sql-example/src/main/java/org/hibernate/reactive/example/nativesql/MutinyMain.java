/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.example.nativesql;

import java.time.LocalDate;

import static java.lang.System.out;
import static java.time.Month.JANUARY;
import static java.time.Month.JUNE;
import static java.time.Month.MAY;
import static javax.persistence.Persistence.createEntityManagerFactory;
import static org.hibernate.reactive.mutiny.Mutiny.SessionFactory;

/**
 * Demonstrates the use of Hibernate Reactive with the
 * {@link io.smallrye.mutiny.Uni Mutiny}-based API.
 *
 * Here we use stateless sessions and handwritten SQL.
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
			factory.withStatelessSession(
					session -> session.withTransaction(
							// persist the Authors with their Books in a transaction
							tx -> session.insertAll( author1, author2, book1, book2, book3 )
					)
			)
					// wait for it to finish
					.await().indefinitely();

			factory.withStatelessSession(
					// retrieve a Book
					session -> session.get( Book.class, book1.getId() )
							// print its title
							.invoke( book -> out.println( book.getTitle() + " is a great book!" ) )
			)
					.await().indefinitely();

			factory.withStatelessSession(
					// retrieve an Author
					session -> session.get( Author.class, author2.getId() )
							// lazily fetch their books
							.chain( author -> session.fetch( author.getBooks() )
									// print some info
									.invoke( books -> {
										out.println( author.getName() + " wrote " + books.size() + " books" );
										books.forEach( book -> out.println( book.getTitle() ) );
									} )
							)
			)
					.await().indefinitely();

			factory.withSession(
					// retrieve the Author lazily from a Book
					session -> session.find( Book.class, book1.getId() )
							// fetch a lazy field of the Book
							.chain( book -> session.fetch( book.getAuthor() )
									// print the lazy field
									.invoke( author -> out.printf( "%s wrote '%s'\n", author.getName(), book1.getTitle() ) )
							)
			)
					.await().indefinitely();

			factory.withStatelessSession(
					// query the Book titles
					session -> session.createNativeQuery(
							"select book.title, author.name from books book join authors author on book.author_id = author.id order by book.title desc",
							Object[].class
					)
							.getResultList()
							.invoke( rows -> rows.forEach(
									row -> out.printf( "%s (%s)\n", row[0], row[1] )
							) )
			)
					.await().indefinitely();

			factory.withStatelessSession(
					// query the entire Book entities
					session -> session.createNativeQuery(
							"select * from books order by title desc",
							Book.class
					)
							.getResultList()
							.invoke( books -> books.forEach(
									b -> out.printf(
											"%s: %s\n",
											b.getIsbn(),
											b.getTitle()
									)
							) )
			)
					.await().indefinitely();

			factory.withStatelessSession(
					session -> session.withTransaction(
							// delete a detached Book
							tx -> session.delete( book2 )
					)
			)
					.await().indefinitely();

			factory.withStatelessSession(
					session -> session.withTransaction(
							// delete all the Books
							tx -> session.createNativeQuery( "delete from books" ).executeUpdate()
									//delete all the Authors
									.chain( () -> session.createNativeQuery( "delete from authors" ).executeUpdate() )
					)
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

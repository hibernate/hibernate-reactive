package org.hibernate.example.reactive;

import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;

import static java.lang.System.out;
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
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		// obtain a reactive session
		factory.withTransaction(
				// persist the Authors with their Books in a transaction
				(session, tx) -> session.persist(author1, author2)
		)
				// wait for it to finish
				.await().indefinitely();

		factory.withSession(
				// retrieve a Book
				session -> session.find(Book.class, book1.id)
						// print its title
						.invoke( book -> out.println(book.title + " is a great book!") )
		)
				.await().indefinitely();

		factory.withSession(
				// retrieve both Authors at once
				session -> session.find(Author.class, author1.id, author2.id)
						.invoke( authors -> authors.forEach( author -> out.println(author.name) ) )
		)
				.await().indefinitely();

		factory.withSession(
				// retrieve an Author
				session -> session.find(Author.class, author2.id)
						// lazily fetch their books
						.flatMap( author -> fetch(author.books)
								// print some info
								.invoke( books -> {
									out.println(author.name + " wrote " + books.size() + " books");
									books.forEach( book -> out.println(book.title) );
								} )
						)
		)
				.await().indefinitely();

		factory.withSession(
				// query the Book titles
				session -> session.createQuery("select title, author.name from Book order by title desc", Object[].class)
						.getResultList()
						.invoke( rows -> rows.forEach(
								row -> out.printf("%s (%s)\n", row[0], row[1])
						) )
		)
				.await().indefinitely();

		factory.withSession(
				// query the entire Book entities
				session -> session.createQuery("from Book book join fetch book.author order by book.title desc", Book.class)
						.getResultList()
						.invoke( books -> books.forEach(
								b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author.name)
						) )
		)
				.await().indefinitely();

		factory.withSession(
				// use a criteria query
				session -> {
					CriteriaQuery<Book> query = factory.getCriteriaBuilder().createQuery(Book.class);
					Root<Author> a = query.from(Author.class);
					Join<Author,Book> b = a.join(Author_.books);
					query.where( a.get(Author_.name).in("Neal Stephenson", "William Gibson") );
					query.select(b);
					return session.createQuery(query).getResultList().invoke(
							books -> books.forEach( book -> out.println(book.title) )
					);
				}
		)
				.await().indefinitely();

		factory.withTransaction(
				// retrieve a Book
				(session, tx) -> session.find(Book.class, book2.id)
						// delete the Book
						.flatMap( book -> session.remove(book) )
		)
				.await().indefinitely();

		factory.withTransaction(
				// delete all the Books in a transaction
				(session, tx) -> session.createQuery("delete Book").executeUpdate()
						//delete all the Authors
						.flatMap( $ -> session.createQuery("delete Author").executeUpdate() )
		)
				.await().indefinitely();

		// remember to shut down the connection pool
		factory.close();
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

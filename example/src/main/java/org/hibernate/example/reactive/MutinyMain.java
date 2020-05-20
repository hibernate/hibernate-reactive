package org.hibernate.example.reactive;

import static java.lang.System.out;
import static javax.persistence.Persistence.createEntityManagerFactory;
import static org.hibernate.reactive.mutiny.Mutiny.SessionFactory;

/**
 * Demonstrates the use of Hibernate Reactive with the
 * {@link io.smallrye.mutiny.Uni Mutiny}-based API.
 */
public class MutinyMain {

	public static void main(String[] args) {

		// obtain a factory for reactive sessions based on the
		// standard JPA configuration properties specified in
		// resources/META-INF/persistence.xml
		SessionFactory sessionFactory =
				createEntityManagerFactory("example")
						.unwrap(SessionFactory.class);

		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", "Iain M. Banks");
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", "Neal Stephenson");

		//obtain a reactive session
		//persist the Books
		sessionFactory.withReactiveSession(
				//persist the Books
				session5 -> session5.withTransaction(tx1 -> session5.persist(book1, book2) )
		)
				//wait for it to finish
				.await().indefinitely();

		//retrieve a Book and print its title
		sessionFactory.withReactiveSession(
				//retrieve a Book and print its title
				session4 -> session4.find(Book.class, book1.id)
						.onItem().invoke( book3 -> out.println(book3.title + " is a great book!") )
		)
				.await().indefinitely();

		//query the Book titles
		sessionFactory.withReactiveSession(
				//query the Book titles
				session3 -> session3.createQuery("select title, author from Book order by title desc", Object[].class)
						.getResultList()
						.onItem().invoke( rows -> rows.forEach(
								row -> out.printf("%s (%s)\n", row[0], row[1])
						) )
		)
				.await().indefinitely();

		//query the entire Book entities
		sessionFactory.withReactiveSession(
				//query the entire Book entities
				session2 -> session2.createQuery("from Book order by title desc", Book.class)
						.getResultList()
						.onItem().invoke( books -> books.forEach(
								b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author)
						) )
		)
				.await().indefinitely();

		//retrieve a Book and delete it
		sessionFactory.withReactiveSession(
				//retrieve a Book and delete it
				session1 -> session1.withTransaction(
						tx -> session1.find(Book.class, book2.id)
								.onItem().produceUni( book -> session1.remove(book) )
				)
		)
				.await().indefinitely();

		//delete all the Books
		sessionFactory.withReactiveSession(
				//delete all the Books
				session -> session.createQuery("delete Book").executeUpdate()
		)
				.await().indefinitely();

		System.exit(0);
	}

}

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
		sessionFactory.withSession(
				//persist the Books
				session -> session.withTransaction(tx -> session.persist(book1, book2) )
		)
				//wait for it to finish
				.await().indefinitely();

		sessionFactory.withSession(
				//retrieve a Book and print its title
				session -> session.find(Book.class, book1.id)
						.onItem().invoke( book -> out.println(book.title + " is a great book!") )
		)
				.await().indefinitely();

		sessionFactory.withSession(
				//retrieve both Books at once
				session -> session.find(Book.class, book1.id, book2.id)
						.onItem().invoke( books -> books.forEach( book -> out.println(book.isbn) ) )
		)
				.await().indefinitely();

		sessionFactory.withSession(
				//query the Book titles
				session -> session.createQuery("select title, author from Book order by title desc", Object[].class)
						.getResultList()
						.onItem().invoke( rows -> rows.forEach(
								row -> out.printf("%s (%s)\n", row[0], row[1])
						) )
		)
				.await().indefinitely();

		sessionFactory.withSession(
				//query the entire Book entities
				session -> session.createQuery("from Book order by title desc", Book.class)
						.getResultList()
						.onItem().invoke( books -> books.forEach(
								b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author)
						) )
		)
				.await().indefinitely();

		sessionFactory.withSession(
				//retrieve a Book and delete it
				session -> session.withTransaction(
						tx -> session.find(Book.class, book2.id)
								.onItem().produceUni( book -> session.remove(book) )
				)
		)
				.await().indefinitely();

		sessionFactory.withSession(
				//delete all the Books
				session -> session.createQuery("delete Book").executeUpdate()
		)
				.await().indefinitely();

		sessionFactory.close();
	}

}

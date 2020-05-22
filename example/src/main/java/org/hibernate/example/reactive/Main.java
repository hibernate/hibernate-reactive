package org.hibernate.example.reactive;

import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.stage.Stage.*;
import static javax.persistence.Persistence.*;
import static java.lang.System.out;

/**
 * Demonstrates the use of Hibernate Reactive with the
 * {@link java.util.concurrent.CompletionStage}-based
 * API.
 */
public class Main {

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
		sessionFactory.withSession(
				//persist the Books
				session5 -> session5.withTransaction(tx1 -> session5.persist(book1, book2) )
		)
				//wait for it to finish
				.toCompletableFuture().join();

		//retrieve a Book and print its title
		sessionFactory.withSession(
				//retrieve a Book and print its title
				session4 -> session4.find(Book.class, book1.id)
						.thenAccept(book3 -> out.println(book3.title + " is a great book!") )
		)
				.toCompletableFuture().join();

		//query the Book titles
		sessionFactory.withSession(
				//query the Book titles
				session3 -> session3.createQuery("select title, author from Book order by title desc", Object[].class)
						.getResultList()
						.thenAccept( rows -> rows.forEach(
								row -> out.printf("%s (%s)\n", row[0], row[1])
						) )
		)
				.toCompletableFuture().join();

		//query the entire Book entities
		sessionFactory.withSession(
				//query the entire Book entities
				session2 -> session2.createQuery("from Book order by title desc", Book.class)
						.getResultList()
						.thenAccept( books -> books.forEach(
								b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author)
						) )
		)
				.toCompletableFuture().join();

		//retrieve a Book and delete it
		sessionFactory.withSession(
				//retrieve a Book and delete it
				session1 -> session1.withTransaction(
						tx -> session1.find(Book.class, book2.id)
								.thenCompose( book -> session1.remove(book) )
				)
		)
				.toCompletableFuture().join();

		//delete all the Books
		sessionFactory.withSession(
				//delete all the Books
				session -> session.createQuery("delete Book").executeUpdate()
		)
				.toCompletableFuture().join();

		sessionFactory.close();
	}

}

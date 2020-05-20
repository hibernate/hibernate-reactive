package org.hibernate.example.reactive;

import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.stage.Stage.*;
import static javax.persistence.Persistence.*;
import static java.lang.System.out;

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
		join( sessionFactory.withReactiveSession(
				//persist the Books
				session -> session.withTransaction( tx -> session.persist(book1, book2) )
		) );

		join( sessionFactory.withReactiveSession(
				//retrieve a Book and print its title
				session -> session.find(Book.class, book1.id)
						.thenAccept( book -> out.println(book.title + " is a great book!") )
		) );

		join( sessionFactory.withReactiveSession(
				//query the Book titles
				session -> session.createQuery("select title, author from Book order by title desc", Object[].class)
						.getResultList()
						.thenAccept( rows -> rows.forEach(
								row -> out.printf("%s (%s)\n", row[0], row[1])
						) )
		) );

		join( sessionFactory.withReactiveSession(
				//query the entire Book entities
				session -> session.createQuery("from Book order by title desc", Book.class)
						.getResultList()
						.thenAccept( books -> books.forEach(
								b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author)
						) )
		) );

		join( sessionFactory.withReactiveSession(
				//retrieve a Book and delete it
				session -> session.withTransaction(
						tx -> session.find(Book.class, book2.id)
								.thenCompose( book -> session.remove(book) )
				)
		) );

		join( sessionFactory.withReactiveSession(
				//delete all the Books
				session -> session.createQuery("delete Book").executeUpdate()
		) );

		System.exit(0);
	}

	private static void join(CompletionStage<?> stage) {
		stage.toCompletableFuture().join();
	}

}

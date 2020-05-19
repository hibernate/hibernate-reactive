package org.hibernate.example.reactive;

import org.hibernate.reactive.stage.Stage;

import static javax.persistence.Persistence.createEntityManagerFactory;

public class Main {

	public static void main(String[] args) {

		// obtain a factory for reactive sessions based on the
		// standard JPA configuration properties specified in
		// resources/META-INF/persistence.xml
		Stage.SessionFactory sessionFactory =
				createEntityManagerFactory("example")
						.unwrap(Stage.SessionFactory.class);

		Book book = new Book("1-85723-235-6", "Feersum Endjinn", "Iain M. Banks");

		//obtain a reactive session
		Stage.Session session1 = sessionFactory.openReactiveSession();
		//persist a Book
		session1.persist(book)
				.thenCompose( $ -> session1.flush() )
				.thenAccept( $ -> session1.close() )
				.toCompletableFuture()
				.join();

		Stage.Session session2 = sessionFactory.openReactiveSession();
		//retrieve the Book and print its title
		session2.find(Book.class, book.id)
				.thenAccept( bOOk -> System.out.println(bOOk.title + " is a great book!") )
				.thenAccept( $ -> session2.close() )
				.toCompletableFuture()
				.join();

		Stage.Session session3 = sessionFactory.openReactiveSession();
		//query the Book titles
		session3.createQuery("select title from Book order by title desc")
				.getResultList()
				.thenAccept(System.out::println)
				.thenAccept( $ -> session3.close() )
				.toCompletableFuture()
				.join();

		Stage.Session session4 = sessionFactory.openReactiveSession();
		//retrieve the Book and delete it
		session4.find(Book.class, book.id)
				.thenCompose( bOOk -> session4.remove(bOOk) )
				.thenCompose( $ -> session4.flush() )
				.thenAccept( $ -> session4.close() )
				.toCompletableFuture()
				.join();

		System.exit(0);
	}

}

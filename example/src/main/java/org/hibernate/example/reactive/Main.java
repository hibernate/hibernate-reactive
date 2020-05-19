package org.hibernate.example.reactive;

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
		Session session1 = sessionFactory.openReactiveSession();
		//persist the Books
		session1.persist(book1, book2)
				.thenCompose( $ -> session1.flush() )
				.thenAccept( $ -> session1.close() )
				.toCompletableFuture()
				.join();

		Session session2 = sessionFactory.openReactiveSession();
		//retrieve a Book and print its title
		session2.find(Book.class, book1.id)
				.thenAccept( book -> out.println(book.title + " is a great book!") )
				.thenAccept( $ -> session2.close() )
				.toCompletableFuture()
				.join();

		Session session3 = sessionFactory.openReactiveSession();
		//query the Book titles
		session3.createQuery("select title, author from Book order by title desc", Object[].class)
				.getResultList()
				.thenAccept( rows -> rows.forEach(
						row -> out.printf("%s (%s)\n", row[0], row[1])
				) )
				.thenAccept( $ -> session3.close() )
				.toCompletableFuture()
				.join();

		Session session4 = sessionFactory.openReactiveSession();
		//query the entire Book entities
		session4.createQuery("from Book order by title desc", Book.class)
				.getResultList()
				.thenAccept( books -> books.forEach(
						b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author)
				) )
				.thenAccept( $ -> session4.close() )
				.toCompletableFuture()
				.join();

		Session session5 = sessionFactory.openReactiveSession();
		//retrieve a Book and delete it
		session5.find(Book.class, book2.id)
				.thenCompose( bOOk -> session5.remove(bOOk) )
				.thenCompose( $ -> session5.flush() )
				.thenAccept( $ -> session5.close() )
				.toCompletableFuture()
				.join();

		Session session6 = sessionFactory.openReactiveSession();
		//delete all the Books
		session6.createQuery("delete Book")
				.executeUpdate()
				.thenAccept( $ -> session6.close() )
				.toCompletableFuture()
				.join();

		System.exit(0);
	}

}

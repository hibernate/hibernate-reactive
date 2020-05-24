package org.hibernate.example.reactive;

import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;

import static java.lang.System.out;
import static javax.persistence.Persistence.createEntityManagerFactory;
import static org.hibernate.reactive.stage.Stage.SessionFactory;
import static org.hibernate.reactive.stage.Stage.fetch;

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

		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		//obtain a reactive session
		sessionFactory.withSession(
				//persist the Authors with their Books in a transaction
				session -> session.withTransaction( tx -> session.persist(author1, author2) )
		)
				//wait for it to finish
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//retrieve a Book
				session -> session.find(Book.class, book1.id)
						//print its title
						.thenAccept( book -> out.println(book.title + " is a great book!") )
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//retrieve both Authors at once
				session -> session.find(Author.class, author1.id, author2.id)
						.thenAccept( authors -> authors.forEach( author -> out.println(author.name) ) )
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//retrieve an Author
				session -> session.find(Author.class, author2.id)
						//lazily fetch their books
						.thenCompose( author -> fetch(author.books)
								//print some info
								.thenAccept( books -> {
									out.println(author.name + " wrote " + books.size() + " books");
									books.forEach( book -> out.println(book.title) );
								} )
						)
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//query the Book titles
				session -> session.createQuery("select title, author.name from Book order by title desc", Object[].class)
						.getResultList()
						.thenAccept( rows -> rows.forEach(
								row -> out.printf("%s (%s)\n", row[0], row[1])
						) )
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//query the entire Book entities
				session -> session.createQuery("from Book book join fetch book.author order by book.title desc", Book.class)
						.getResultList()
						.thenAccept( books -> books.forEach(
								b -> out.printf("%s: %s (%s)\n", b.isbn, b.title, b.author.name)
						) )
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//use a criteria query
				session -> {
					CriteriaQuery<Book> query = sessionFactory.getCriteriaBuilder().createQuery(Book.class);
					Root<Author> a = query.from(Author.class);
					Join<Author,Book> b = a.join(Author_.books);
					query.where( a.get("name").in("Neal Stephenson", "William Gibson") );
					query.select(b);
					return session.createQuery(query).getResultList().thenAccept(
							books -> books.forEach( book -> out.println(book.title) )
					);
				}
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//retrieve a Book and delete it
				session -> session.withTransaction(
						tx -> session.find(Book.class, book2.id)
								.thenCompose( book -> session.remove(book) )
				)
		)
				.toCompletableFuture().join();

		sessionFactory.withSession(
				//delete everything in a transaction
				session -> session.withTransaction(
						//delete all the Books
						tx -> session.createQuery("delete Book").executeUpdate()
								//delete all the Authors
								.thenCompose( $ -> session.createQuery("delete Author").executeUpdate() )
				)
		)
				.toCompletableFuture().join();

		sessionFactory.close();
	}

}

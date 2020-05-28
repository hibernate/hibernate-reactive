/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.ColumnResult;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.SqlResultSetMapping;
import javax.persistence.Table;
import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;

import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.FetchType.LAZY;

public class QueryTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass(Author.class);
		configuration.addAnnotatedClass(Book.class);
		return configuration;
	}

	@Test
	public void testCriteriaEntityQuery(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> query = builder.createQuery(Book.class);
		Root<Book> b = query.from(Book.class);
		b.fetch("author");
		query.orderBy( builder.asc( b.get("isbn") ) );

		CriteriaUpdate<Book> update = builder.createCriteriaUpdate(Book.class);
		b = update.from(Book.class);
		update.set( b.get("title"), "XXX" );

		CriteriaDelete<Book> delete = builder.createCriteriaDelete(Book.class);
		b = delete.from(Book.class);

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2) )
						.thenCompose( session -> session.flush() )
						.whenComplete( (session,err) -> session.close() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(query).getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( book -> {
								context.assertNotNull( book.id );
								context.assertNotNull( book.title );
								context.assertNotNull( book.isbn );
							} );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(update).executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(delete).executeUpdate() )
		);
	}

	@Test
	public void testCriteriaProjectionQuery(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
		CriteriaQuery<Tuple> query = builder.createQuery(Tuple.class);
		Root<Book> b = query.from(Book.class);
		Join<Object, Object> a = b.join("author");
		query.orderBy( builder.asc( b.get("isbn") ) );
		query.multiselect( b.get("title").alias("t"), a.get("name").alias("n") );
		query.where( a.get("name").in("Neal Stephenson", "William Gibson") );

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2) )
						.thenCompose( session -> session.flush() )
						.whenComplete( (session,err) -> session.close() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(query).getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 2, books.size() );
							books.forEach( book -> {
								context.assertNotNull( book.get("t") );
								context.assertNotNull( book.get("n") );
							} );
						} )
		);
	}

	@Test
	public void testNativeEntityQuery(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		test(context,
				openSession()
					.thenCompose( session -> session.persist(author1, author2) )
					.thenCompose( session -> session.flush() )
					.whenComplete( (session,err) -> session.close() )
					.thenCompose( v -> openSession() )
					.thenCompose( session -> session.createNativeQuery("select * from books order by isbn", Book.class).getResultList() )
					.thenAccept( books -> {
						context.assertEquals( 3, books.size() );
						books.forEach( book -> {
							context.assertNotNull( book.id );
							context.assertNotNull( book.title );
							context.assertNotNull( book.isbn );
						} );
					} )
		);
	}

	@Test
	public void testNativeProjectionQuery(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2) )
						.thenCompose( session -> session.flush() )
						.whenComplete( (session,err) -> session.close() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery("select b.title, a.name from books b join authors a on author_id=a.id order by b.isbn", "title,author").getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( book -> {
								context.assertTrue( book instanceof Object[] );
								Object[] tuple = (Object[]) book;
								context.assertEquals( 2, tuple.length );
								context.assertTrue( tuple[0] instanceof String );
								context.assertTrue( tuple[1] instanceof String );
							} );
						} )
		);
	}

	@Test
	public void testNamedHqlProjectionQuery(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2) )
						.thenCompose( session -> session.flush() )
						.whenComplete( (session,err) -> session.close() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNamedQuery("title,author (hql)").getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( book -> {
								context.assertTrue( book instanceof Object[] );
								Object[] tuple = (Object[]) book;
								context.assertEquals( 2, tuple.length );
								context.assertTrue( tuple[0] instanceof String );
								context.assertTrue( tuple[1] instanceof String );
							} );
						} )
		);
	}

	@Test
	public void testNamedNativeProjectionQuery(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1);
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2);
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2);
		author1.books.add(book1);
		author2.books.add(book2);
		author2.books.add(book3);

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2) )
						.thenCompose( session -> session.flush() )
						.whenComplete( (session,err) -> session.close() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNamedQuery("title,author (sql)").getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( book -> {
								context.assertTrue( book instanceof Object[] );
								Object[] tuple = (Object[]) book;
								context.assertEquals( 2, tuple.length );
								context.assertTrue( tuple[0] instanceof String );
								context.assertTrue( tuple[1] instanceof String );
							} );
						} )
		);
	}

	@NamedNativeQuery(
			name = "title,author (sql)",
			query = "select b.title, a.name from books b join authors a on author_id=a.id order by b.isbn",
			resultSetMapping = "title,author"
	)

	@NamedQuery(
			name = "title,author (hql)",
			query = "select b.title, a.name from Book b join b.author a order by b.isbn"
	)

	@SqlResultSetMapping(name="title,author", columns={
			@ColumnResult(name = "title",type=String.class),
			@ColumnResult(name = "name",type=String.class)
	})

	@Entity(name="Author")
	@Table(name="authors")
	static class Author {
		@Id
		@GeneratedValue
		Integer id;

		String name;

		@OneToMany(mappedBy = "author", cascade = PERSIST)
		List<Book> books = new ArrayList<>();

		Author(String name) {
			this.name = name;
		}

		Author() {}
	}

	@Entity(name="Book")
	@Table(name="books")
	static class Book {
		@Id @GeneratedValue Integer id;

		String isbn;

		String title;

		@ManyToOne(fetch = LAZY)
		Author author;

		Book(String isbn, String title, Author author) {
			this.title = title;
			this.isbn = isbn;
			this.author = author;
		}

		Book() {}
	}

}

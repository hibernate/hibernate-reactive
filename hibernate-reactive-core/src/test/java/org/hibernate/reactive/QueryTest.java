/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.ColumnResult;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQuery;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.OneToMany;
import javax.persistence.SqlResultSetMapping;
import javax.persistence.Table;
import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.ParameterExpression;
import javax.persistence.criteria.Root;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.FetchType.LAZY;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class QueryTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass(Author.class);
		configuration.addAnnotatedClass(Book.class);
		return configuration;
	}

	@After
	public void cleanDB(TestContext context) {
		test( context, deleteEntities( "Book", "Author" ) );
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
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
	public void testCriteriaEntityQueryWithParam(TestContext context) {
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
		ParameterExpression<String> t = builder.parameter(String.class);
		query.where( builder.equal( b.get("title"), t ) );
		query.orderBy( builder.asc( b.get("isbn") ) );

		CriteriaUpdate<Book> update = builder.createCriteriaUpdate(Book.class);
		b = update.from(Book.class);
		update.where( builder.equal( b.get("title"), t ) );
		update.set( b.get("title"), "XXX" );

		CriteriaDelete<Book> delete = builder.createCriteriaDelete(Book.class);
		b = delete.from(Book.class);
		delete.where( builder.equal( b.get("title"), t ) );

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(query)
								.setParameter( t, "Snow Crash")
								.getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 1, books.size() );
							books.forEach( book -> {
								context.assertNotNull( book.id );
								context.assertNotNull( book.title );
								context.assertNotNull( book.isbn );
								context.assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(update)
								.setParameter( t, "Snow Crash")
								.executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(delete)
								.setParameter( t, "Snow Crash")
								.executeUpdate() )
		);
	}

	@Test
	public void testCriteriaEntityQueryWithNamedParam(TestContext context) {
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
		ParameterExpression<String> t = builder.parameter(String.class, "title");
		query.where( builder.equal( b.get("title"), t ) );
		query.orderBy( builder.asc( b.get("isbn") ) );

		CriteriaUpdate<Book> update = builder.createCriteriaUpdate(Book.class);
		b = update.from(Book.class);
		update.where( builder.equal( b.get("title"), t ) );
		update.set( b.get("title"), "XXX" );

		CriteriaDelete<Book> delete = builder.createCriteriaDelete(Book.class);
		b = delete.from(Book.class);
		delete.where( builder.equal( b.get("title"), t ) );

		test(context,
				openSession()
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(query)
								.setParameter("title", "Snow Crash")
								.getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 1, books.size() );
							books.forEach( book -> {
								context.assertNotNull( book.id );
								context.assertNotNull( book.title );
								context.assertNotNull( book.isbn );
								context.assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(update)
								.setParameter("title", "Snow Crash")
								.executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery(delete)
								.setParameter("title", "Snow Crash")
								.executeUpdate() )
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
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
	public void testNativeEntityQueryWithParam(TestContext context) {
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery("select * from books where title=?1 order by isbn", Book.class)
								.setParameter(1, "Snow Crash")
								.getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 1, books.size() );
							books.forEach( book -> {
								context.assertNotNull( book.id );
								context.assertNotNull( book.title );
								context.assertNotNull( book.isbn );
								context.assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery("update books set title = ?1 where title = ?2")
								.setParameter(1, "XXX")
								.setParameter(2, "Snow Crash")
								.executeUpdate() )
						.thenAccept( count -> context.assertEquals(1, count) )
		);
	}

	@Test
	public void testNativeEntityQueryWithNamedParam(TestContext context) {
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery("select * from books where title=:title order by isbn", Book.class)
								.setParameter("title", "Snow Crash")
								.getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 1, books.size() );
							books.forEach( book -> {
								context.assertNotNull( book.id );
								context.assertNotNull( book.title );
								context.assertNotNull( book.isbn );
								context.assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery("update books set title = :newtitle where title = :title")
								.setParameter("newtitle", "XXX")
								.setParameter("title", "Snow Crash")
								.executeUpdate() )
						.thenAccept( count -> context.assertEquals(1, count) )
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
								"select b.title, a.name from books b join authors a on author_id=a.id order by b.isbn",
								session.getResultSetMapping(Object[].class, "title,author")
						).getResultList())
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( tuple -> {
								context.assertTrue( tuple instanceof Object[] );
								context.assertEquals( 2, tuple.length );
								context.assertTrue( tuple[0] instanceof String );
								context.assertTrue( tuple[1] instanceof String );
							} );
						} )

						.thenCompose( v -> openSession() ).thenCompose(
								session -> session.createQuery( "select title from Book", String.class )
										.getResultList()
										.thenAccept( list -> context.assertTrue( list.get(0) instanceof String ) )
										.thenCompose( vv -> session.createQuery("select title, isbn, id from Book", Object[].class )
												.getResultList()
												.thenAccept( list -> {
													Object[] tuple = list.get(0);
													context.assertEquals( 3, tuple.length );
													context.assertTrue( tuple[0] instanceof String );
												} )
										)
										.thenCompose( vv -> session.createNativeQuery( "select title from books" )
												.getResultList()
												.thenAccept( list -> context.assertTrue( list.get(0) instanceof String ) )
										)
										.thenCompose( vv -> session.createNativeQuery( "select title from books", String.class )
												.getResultList()
												.thenAccept( list -> context.assertTrue( list.get(0) instanceof String ) )
										)
										.thenCompose( vv -> session.createNativeQuery("select title, isbn, id from books" )
												.getResultList()
												.thenAccept( list -> {
													Object[] tuple = (Object[]) list.get(0);
													context.assertEquals( 3, tuple.length );
													context.assertTrue( tuple[0] instanceof String );
												} )
										)
										.thenCompose( vv -> session.createNativeQuery("select title, isbn, id from books", Object[].class )
												.getResultList()
												.thenAccept( list -> {
													Object[] tuple = list.get(0);
													context.assertEquals( 3, tuple.length );
													context.assertTrue( tuple[0] instanceof String );
												} )
										)
										.thenCompose( vv -> session.createNativeQuery("select title, isbn, id from books", Tuple.class )
												.getResultList()
												.thenAccept( list -> {
													Tuple tuple = list.get(0);
													context.assertEquals( 3, tuple.toArray().length );
													context.assertTrue( tuple.get(0) instanceof String );
													context.assertTrue( tuple.get("isbn") instanceof String );
												} )
										)
								)
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNamedQuery("title,author (hql)", Object[].class).getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( tuple -> {
								context.assertTrue( tuple instanceof Object[] );
								context.assertEquals( 2, tuple.length );
								context.assertTrue( tuple[0] instanceof String );
								context.assertTrue( tuple[1] instanceof String );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery("update Book set title = ?1 where title = ?2")
								.setParameter(1, "XXX")
								.setParameter(2, "Snow Crash")
								.executeUpdate() )
								.thenAccept( count -> context.assertEquals(1, count) )
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
						.thenCompose( session -> session.persist(author1, author2)
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNamedQuery("title,author (sql)", Object[].class).getResultList() )
						.thenAccept( books -> {
							context.assertEquals( 3, books.size() );
							books.forEach( tuple -> {
								context.assertTrue( tuple instanceof Object[] );
								context.assertEquals( 2, tuple.length );
								context.assertTrue( tuple[0] instanceof String );
								context.assertTrue( tuple[1] instanceof String );
							} );
						} )
		);
	}

	@Test
	public void testScalarQuery(TestContext context) {
		test(context, openSession()
				.thenCompose( s -> s.createNativeQuery( selectCurrentTimestampQuery() ).getSingleResult() )
				.thenAccept( r -> context.assertTrue( r instanceof OffsetDateTime || r instanceof LocalDateTime ) )
		);
	}

	private String selectCurrentTimestampQuery() {
		switch ( dbType() ) {
			case DB2:
				return "select current_timestamp from sysibm.dual";
			case ORACLE:
				return "select current_date from dual";
			default:
				return "select current_timestamp";
		}
	}

	@Test
	public void testSingleResultQueryNull(TestContext context) {
		test(context, openSession()
				.thenCompose(s -> s.createQuery("from Book").getSingleResultOrNull())
				.thenAccept(context::assertNull)
		);
	}

	@Test
	public void testSingleResultQueryException(TestContext context) {
		test(context, openSession()
				.thenCompose(s -> s.createQuery("from Book").getSingleResult())
				.whenComplete((r, x) -> {
					context.assertNull(r);
					context.assertNotNull(x);

				})
				.handle((r,x) -> {
					context.assertTrue(x.getCause() instanceof NoResultException);
					return null;
				})
		);
	}

	@Test
	public void testSingleResultMultipleException(TestContext context) {
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		test(context, openSession()
				.thenCompose(s -> s.persist(author1, author2).thenCompose(v -> s.flush()))
				.thenCompose(v -> openSession())
				.thenCompose(s -> s.createQuery("from Author").getSingleResult())
				.whenComplete((r, x) -> {
					context.assertNull(r);
					context.assertNotNull(x);

				})
				.handle((r,x) -> {
					context.assertTrue(x.getCause() instanceof NonUniqueResultException);
					return null;
				})
				.thenCompose(v -> openSession())
				.thenCompose(s -> s.createQuery("from Author").getSingleResultOrNull())
				.whenComplete((r, x) -> {
					context.assertNull(r);
					context.assertNotNull(x);

				})
				.handle((r,x) -> {
					context.assertTrue(x.getCause() instanceof NonUniqueResultException);
					return null;
				})
		);
	}

	@NamedNativeQuery(
			name = "title,author (sql)",
			resultClass = Object[].class,
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

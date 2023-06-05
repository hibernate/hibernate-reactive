/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.ColumnResult;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.NamedNativeQuery;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.NoResultException;
import jakarta.persistence.OneToMany;
import jakarta.persistence.SqlResultSetMapping;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.ParameterExpression;
import jakarta.persistence.criteria.Root;

import static jakarta.persistence.CascadeType.PERSIST;
import static jakarta.persistence.FetchType.LAZY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.QueryTest.Author.AUTHOR_TABLE;
import static org.hibernate.reactive.QueryTest.Author.HQL_NAMED_QUERY;
import static org.hibernate.reactive.QueryTest.Author.SQL_NAMED_QUERY;
import static org.hibernate.reactive.QueryTest.Book.BOOK_TABLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)
public class QueryTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Test
	public void testCriteriaEntityQuery(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> query = builder.createQuery( Book.class );
		Root<Book> b = query.from( Book.class );
		b.fetch( "author" );
		query.orderBy( builder.asc( b.get( "isbn" ) ) );

		CriteriaUpdate<Book> update = builder.createCriteriaUpdate( Book.class );
		b = update.from( Book.class );
		update.set( b.get( "title" ), "XXX" );

		CriteriaDelete<Book> delete = builder.createCriteriaDelete( Book.class );
		b = delete.from( Book.class );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( query ).getResultList() )
						.thenAccept( books -> {
							assertEquals( 3, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
							} );
						} )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( update ).executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( delete ).executeUpdate() )
		);
	}

	@Test
	public void testCriteriaEntityQueryWithParam(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> query = builder.createQuery( Book.class );
		Root<Book> b = query.from( Book.class );
		b.fetch( "author" );
		ParameterExpression<String> t = builder.parameter( String.class );
		query.where( builder.equal( b.get( "title" ), t ) );
		query.orderBy( builder.asc( b.get( "isbn" ) ) );

		CriteriaUpdate<Book> update = builder.createCriteriaUpdate( Book.class );
		b = update.from( Book.class );
		update.where( builder.equal( b.get( "title" ), t ) );
		update.set( b.get( "title" ), "XXX" );

		CriteriaDelete<Book> delete = builder.createCriteriaDelete( Book.class );
		b = delete.from( Book.class );
		delete.where( builder.equal( b.get( "title" ), t ) );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( query )
								.setParameter( t, "Snow Crash" )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 1, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
								assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( update )
								.setParameter( t, "Snow Crash" )
								.executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( delete )
								.setParameter( t, "Snow Crash" )
								.executeUpdate() )
		);
	}

	@Test
	public void testCriteriaEntityQueryWithNamedParam(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> query = builder.createQuery( Book.class );
		Root<Book> b = query.from( Book.class );
		b.fetch( "author" );
		ParameterExpression<String> t = builder.parameter( String.class, "title" );
		query.where( builder.equal( b.get( "title" ), t ) );
		query.orderBy( builder.asc( b.get( "isbn" ) ) );

		CriteriaUpdate<Book> update = builder.createCriteriaUpdate( Book.class );
		b = update.from( Book.class );
		update.where( builder.equal( b.get( "title" ), t ) );
		update.set( b.get( "title" ), "XXX" );

		CriteriaDelete<Book> delete = builder.createCriteriaDelete( Book.class );
		b = delete.from( Book.class );
		delete.where( builder.equal( b.get( "title" ), t ) );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( query )
								.setParameter( "title", "Snow Crash" )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 1, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
								assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( update )
								.setParameter( "title", "Snow Crash" )
								.executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( delete )
								.setParameter( "title", "Snow Crash" )
								.executeUpdate() )
		);
	}

	@Test
	public void testCriteriaProjectionQuery(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
		CriteriaQuery<Tuple> query = builder.createQuery( Tuple.class );
		Root<Book> b = query.from( Book.class );
		Join<Object, Object> a = b.join( "author" );
		query.orderBy( builder.asc( b.get( "isbn" ) ) );
		query.multiselect( b.get( "title" ).alias( "t" ), a.get( "name" ).alias( "n" ) );
		query.where( a.get( "name" ).in( "Neal Stephenson", "William Gibson" ) );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( query ).getResultList() )
						.thenAccept( books -> {
							assertEquals( 2, books.size() );
							books.forEach( book -> {
								assertNotNull( book.get( "t" ) );
								assertNotNull( book.get( "n" ) );
							} );
						} )
		);
	}

	@Test
	public void testNativeEntityQuery(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
								"select * from " + BOOK_TABLE + " order by isbn",
								Book.class
						).getResultList() )
						.thenAccept( books -> {
							assertEquals( 3, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
							} );
						} )
		);
	}

	@Test
	public void testNativeEntityQueryWithParam(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
										"select * from " + BOOK_TABLE + " where title=?1 order by isbn",
										Book.class
								)
								.setParameter( 1, "Snow Crash" )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 1, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
								assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
										"update " + BOOK_TABLE + " set title = ?1 where title = ?2" )
								.setParameter( 1, "XXX" )
								.setParameter( 2, "Snow Crash" )
								.executeUpdate() )
						.thenAccept( count -> assertEquals( 1, count ) )
		);
	}

	@Test
	public void testNativeEntityQueryWithNamedParam(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
										"select * from " + BOOK_TABLE + " where title=:title order by isbn",
										Book.class
								)
								.setParameter( "title", "Snow Crash" )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 1, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
								assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
										"update " + BOOK_TABLE + " set title = :newtitle where title = :title" )
								.setParameter( "newtitle", "XXX" )
								.setParameter( "title", "Snow Crash" )
								.executeUpdate() )
						.thenAccept( count -> assertEquals( 1, count ) )
		);
	}

	@Test
	public void testNativeProjectionQuery(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( author1, author2 )
						.thenCompose( v -> session.flush() )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.createNativeQuery(
						"select b.title, a.name from " + BOOK_TABLE + " b join " + AUTHOR_TABLE + " a on author_id=a.id order by b.isbn",
						session.getResultSetMapping( Object[].class, "title,author" )
				).getResultList() )
				.thenAccept( books -> {
					assertEquals( 3, books.size() );
					books.forEach( tuple -> {
						assertTrue( tuple instanceof Object[] );
						assertEquals( 2, tuple.length );
						assertTrue( tuple[0] instanceof String );
						assertTrue( tuple[1] instanceof String );
					} );
				} )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.createQuery( "select title from Book", String.class )
						.getResultList()
						.thenAccept( list -> assertThat( list )
								.containsExactlyInAnyOrder( book1.title, book2.title, book3.title ) )
						.thenCompose( vv -> session
								.createQuery( "select title, isbn, id from Book", Object[].class )
								.getResultList()
								.thenAccept( list -> {
									Object[] tuple = list.get( 0 );
									assertEquals( 3, tuple.length );
									assertTrue( tuple[0] instanceof String );
								} ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title from " + BOOK_TABLE )
								.getResultList()
								.thenAccept( list -> assertTrue( list.get( 0 ) instanceof String ) )
						)
						.thenCompose( vv -> session
								.createNativeQuery( "select title from " + BOOK_TABLE, String.class )
								.getResultList()
								.thenAccept( list -> assertTrue( list.get( 0 ) instanceof String ) ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title, isbn, id from " + BOOK_TABLE )
								.getResultList()
								.thenAccept( list -> {
									Object[] tuple = (Object[]) list.get( 0 );
									assertEquals( 3, tuple.length );
									assertTrue( tuple[0] instanceof String );
								} ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title, isbn, id from " + BOOK_TABLE, Object[].class )
								.getResultList()
								.thenAccept( list -> {
									Object[] tuple = list.get( 0 );
									assertEquals( 3, tuple.length );
									assertTrue( tuple[0] instanceof String );
								} ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title, isbn, id from " + BOOK_TABLE, Tuple.class )
								.getResultList()
								.thenAccept( list -> {
									Tuple tuple = list.get( 0 );
									assertEquals( 3, tuple.toArray().length );
									assertTrue( tuple.get( 0 ) instanceof String );
									assertTrue( tuple.get( "isbn" ) instanceof String );
								} ) )
				)
		);
	}

	@Test
	public void testNamedHqlProjectionQuery(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNamedQuery( HQL_NAMED_QUERY, Object[].class )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 3, books.size() );
							books.forEach( tuple -> {
								assertTrue( tuple instanceof Object[] );
								assertEquals( 2, tuple.length );
								assertTrue( tuple[0] instanceof String );
								assertTrue( tuple[1] instanceof String );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( "update Book set title = ?1 where title = ?2" )
								.setParameter( 1, "XXX" )
								.setParameter( 2, "Snow Crash" )
								.executeUpdate() )
						.thenAccept( count -> assertEquals( 1, count ) )
		);
	}

	@Test
	public void testNamedNativeProjectionQuery(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNamedQuery( SQL_NAMED_QUERY, Object[].class )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 3, books.size() );
							books.forEach( tuple -> {
								assertTrue( tuple instanceof Object[] );
								assertEquals( 2, tuple.length );
								assertTrue( tuple[0] instanceof String );
								assertTrue( tuple[1] instanceof String );
							} );
						} )
		);
	}

	@Test
	public void testScalarQuery(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.createNativeQuery( selectCurrentTimestampQuery() ).getSingleResult() )
				.thenAccept( r -> assertThat( r ).isInstanceOfAny( Timestamp.class, OffsetDateTime.class, LocalDateTime.class ) )
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
	public void testSingleResultQueryNull(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.createQuery( "from Book" ).getSingleResultOrNull() )
				.thenAccept( Assertions::assertNull )
		);
	}

	@Test
	public void testSingleResultQueryException(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.createQuery( "from Book" ).getSingleResult() )
				.whenComplete( (r, x) -> {
					assertNull( r );
					assertNotNull( x );

				} )
				.handle( (r, x) -> {
					assertTrue( x.getCause() instanceof NoResultException );
					return null;
				} )
		);
	}

	@Test
	public void testSingleResultNonUniqueException(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		test( context, openSession()
				.thenCompose( s -> s.persist( author1, author2 ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> assertThrown(
						jakarta.persistence.NonUniqueResultException.class,
						s.createQuery( "from Author" ).getSingleResult()
				) )
		);
	}

	@Test
	public void testSingleResultOrNullNonUniqueException(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		test( context, openSession()
				.thenCompose( s -> s.persist( author1, author2 ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> assertThrown(
						jakarta.persistence.NonUniqueResultException.class,
						s.createQuery( "from Author" ).getSingleResultOrNull()
				) )
		);
	}

	@NamedNativeQuery(
			name = SQL_NAMED_QUERY,
			resultClass = Object[].class,
			query = "select b.title, a.name from " + BOOK_TABLE + " b join " + AUTHOR_TABLE + " a on author_id=a.id order by b.isbn",
			resultSetMapping = "title,author"
	)

	@NamedQuery(
			name = HQL_NAMED_QUERY,
			query = "select b.title, a.name from Book b join b.author a order by b.isbn"
	)

	@SqlResultSetMapping(name = "title,author", columns = {
			@ColumnResult(name = "title", type = String.class),
			@ColumnResult(name = "name", type = String.class)
	})

	@Entity(name = "Author")
	@Table(name = AUTHOR_TABLE)
	static class Author {
		public static final String HQL_NAMED_QUERY = "title,author (hql)";
		public static final String SQL_NAMED_QUERY = "title,author (sql)";

		public static final String AUTHOR_TABLE = "AuthorForQueryTest";

		@Id
		@GeneratedValue
		Integer id;

		String name;

		@OneToMany(mappedBy = "author", cascade = PERSIST)
		List<Book> books = new ArrayList<>();

		Author(String name) {
			this.name = name;
		}

		Author() {
		}
	}

	@Entity(name = "Book")
	@Table(name = BOOK_TABLE)
	static class Book {
		public static final String BOOK_TABLE = "BookForQueryTest";

		@Id
		@GeneratedValue
		Integer id;

		String isbn;

		String title;

		@ManyToOne(fetch = LAZY)
		Author author;

		Book(String isbn, String title, Author author) {
			this.title = title;
			this.isbn = isbn;
			this.author = author;
		}

		Book() {
		}
	}

}

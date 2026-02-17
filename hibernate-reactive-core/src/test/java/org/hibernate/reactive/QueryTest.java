/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

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
import static org.assertj.core.groups.Tuple.tuple;
import static org.hibernate.reactive.QueryTest.Author.AUTHOR_TABLE;
import static org.hibernate.reactive.QueryTest.Author.HQL_NAMED_QUERY;
import static org.hibernate.reactive.QueryTest.Author.SQL_NAMED_QUERY;
import static org.hibernate.reactive.QueryTest.Book.BOOK_TABLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

@Timeout(value = 10, timeUnit = MINUTES)
public class QueryTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	private final static BigDecimal PIE = BigDecimal.valueOf( 3.1416 );
	private final static BigDecimal TAO = BigDecimal.valueOf( 6.2832 );

	@Test
	public void testBigDecimalAsParameter(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1 );
		book1.quantity = BigDecimal.valueOf( 11.2 );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2 );
		book2.quantity = PIE;
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2 );
		book3.quantity = TAO;

		author1.books.add( book1 );
		author2.books.add( book2 );
		author2.books.add( book3 );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( author1, author2 ) )
				// HQL with named parameters
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "from Book where quantity > :quantity", Book.class )
						.setParameter( "quantity", PIE )
						.getResultList()
						.invoke( result -> assertThat( result ).containsExactlyInAnyOrder( book1, book3 ) )
				) )
				// HQL with positional parameters
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "from Book where quantity > ?1", Book.class )
						.setParameter( 1, PIE )
						.getResultList()
						.invoke( result -> assertThat( result ).containsExactlyInAnyOrder( book1, book3 ) )
				) )
				// Criteria
				.call( () -> {
					CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
					CriteriaQuery<Book> query = builder.createQuery( Book.class );
					Root<Book> b = query.from( Book.class );
					b.fetch( "author" );
					query.where( builder.between(
							b.get( "quantity" ),
							BigDecimal.valueOf( 4.0 ),
							BigDecimal.valueOf( 100.0 )
					) );
					return getMutinySessionFactory().withTransaction( s -> s
							.createQuery( query )
							.getResultList()
							.invoke( result -> assertThat( result ).containsExactlyInAnyOrder( book1, book3 ) )
					);
				} )
		);
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
		delete.from( Book.class );

		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( query ).getResultList() )
						.thenAccept( books -> {
							assertThat( books ).hasSize( 3 );
							books.forEach( book -> {
								assertThat( book.id ).isNotNull();
								assertThat( book.title ).isNotNull();
								assertThat( book.isbn ).isNotNull();
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
							assertThat( books ).hasSize( 1 );
							books.forEach( book -> {
								assertThat( book.id ).isNotNull();
								assertThat( book.title ).isNotNull();
								assertThat( book.isbn ).isNotNull();
								assertThat( book.title ).isEqualTo( "Snow Crash" );
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
							assertThat( books ).hasSize( 1 );
							books.forEach( book -> {
								assertThat( book.id ).isNotNull();
								assertThat( book.title ).isNotNull();
								assertThat( book.isbn ).isNotNull();
								assertThat( book.title ).isEqualTo( "Snow Crash" );
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
							assertThat( books ).hasSize( 2 );
							books.forEach( book -> {
								assertThat( book.get( "t" ) );
								assertThat( book.get( "n" ) );
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
							assertThat( books ).hasSize( 3 );
							books.forEach( book -> {
								assertThat( book.id ).isNotNull();
								assertThat( book.title ).isNotNull();
								assertThat( book.isbn ).isNotNull();
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
							assertThat( books ).hasSize( 1 );
							books.forEach( book -> {
								assertThat( book.id ).isNotNull();
								assertThat( book.title ).isNotNull();
								assertThat( book.isbn ).isNotNull();
								assertThat( book.title ).isEqualTo( "Snow Crash" );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
										"update " + BOOK_TABLE + " set title = ?1 where title = ?2" )
								.setParameter( 1, "XXX" )
								.setParameter( 2, "Snow Crash" )
								.executeUpdate() )
						.thenAccept( count -> assertThat( count ).isEqualTo( 1 ) )
		);
	}

	// https://github.com/hibernate/hibernate-reactive/issues/2314
	@Test
	public void testNativeEntityQueryWithLimit(VertxTestContext context) {
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
								)
								.setMaxResults( 2 )
								.getResultList() )
						.thenAccept( books -> {
							assertThat( books )
									.extracting( b -> b.id, b -> b.title, b -> b.isbn )
									.containsExactly(
											tuple( book2.id, book2.title, book2.isbn ),
											tuple( book3.id, book3.title, book3.isbn )
									);
						} )
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
							assertThat( books ).hasSize( 1 );
							books.forEach( book -> {
								assertThat( book.id ).isNotNull();
								assertThat( book.title ).isNotNull();
								assertThat( book.isbn ).isNotNull();
								assertThat( book.title ).isEqualTo( "Snow Crash" );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createNativeQuery(
										"update " + BOOK_TABLE + " set title = :newtitle where title = :title" )
								.setParameter( "newtitle", "XXX" )
								.setParameter( "title", "Snow Crash" )
								.executeUpdate() )
						.thenAccept( count -> assertThat( count ).isEqualTo( 1 ) )
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
					assertThat( books ).hasSize( 3 );
					books.forEach( tuple -> {
						assertThat( tuple ).isInstanceOf( Object[].class );
						assertThat( tuple ).hasSize( 2 );
						assertThat( tuple[0] ).isInstanceOf( String.class );
						assertThat( tuple[1] ).isInstanceOf( String.class );
					} );
				} )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.createSelectionQuery( "select title from Book", String.class )
						.getResultList()
						.thenAccept( list -> assertThat( list )
								.containsExactlyInAnyOrder( book1.title, book2.title, book3.title ) )
						.thenCompose( vv -> session
								.createSelectionQuery( "select title, isbn, id from Book", Object[].class )
								.getResultList()
								.thenAccept( list -> {
									Object[] tuple = list.get( 0 );
									assertThat( tuple ).hasSize( 3 );
									assertThat( tuple[0] ).isInstanceOf( String.class );
								} ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title from " + BOOK_TABLE )
								.getResultList()
								.thenAccept( list -> assertThat( list.get( 0 ) ).isInstanceOf( String.class ) )
						)
						.thenCompose( vv -> session
								.createNativeQuery( "select title from " + BOOK_TABLE, String.class )
								.getResultList()
								.thenAccept( list -> assertThat( list.get( 0 ) ).isInstanceOf( String.class ) ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title, isbn, id from " + BOOK_TABLE )
								.getResultList()
								.thenAccept( list -> {
									Object[] tuple = (Object[]) list.get( 0 );
									assertThat( tuple ).hasSize( 3 );
									assertThat( tuple[0] ).isInstanceOf( String.class );
								} ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title, isbn, id from " + BOOK_TABLE, Object[].class )
								.getResultList()
								.thenAccept( list -> {
									Object[] tuple = list.get( 0 );
									assertThat( tuple ).hasSize( 3 );
									assertThat( tuple[0] ).isInstanceOf( String.class );
								} ) )
						.thenCompose( vv -> session
								.createNativeQuery( "select title, isbn, id from " + BOOK_TABLE, Tuple.class )
								.getResultList()
								.thenAccept( list -> {
									Tuple tuple = list.get( 0 );
									assertThat( tuple.toArray() ).hasSize( 3 );
									assertThat( tuple.get( 0 ) ).isInstanceOf( String.class );
									assertThat( tuple.get( "isbn" ) ).isInstanceOf( String.class );
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
							assertThat( books ).hasSize( 3 );
							books.forEach( tuple -> {
								assertThat( tuple ).isInstanceOf( Object[].class );
								assertThat( tuple ).hasSize( 2 );
								assertThat( tuple[0] ).isInstanceOf( String.class );
								assertThat( tuple[1] ).isInstanceOf( String.class );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createMutationQuery( "update Book set title = ?1 where title = ?2" )
								.setParameter( 1, "XXX" )
								.setParameter( 2, "Snow Crash" )
								.executeUpdate() )
						.thenAccept( count -> assertThat( count ).isEqualTo( 1 ) )
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
							assertThat( books ).hasSize( 3 );
							books.forEach( tuple -> {
								assertThat( tuple ).isInstanceOf( Object[].class );
								assertThat( tuple ).hasSize( 2 );
								assertThat( tuple[0] ).isInstanceOf( String.class );
								assertThat( tuple[1] ).isInstanceOf( String.class );
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
				.thenCompose( s -> s.createSelectionQuery( "from Book", Book.class ).getSingleResultOrNull() )
				.thenAccept( Assertions::assertNull )
		);
	}

	@Test
	public void testSingleResultQueryException(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.createSelectionQuery( "from Book", Book.class ).getSingleResult() )
				.whenComplete( (r, x) -> {
					assertThat( r ).isNull();
					assertThat( x ).isNotNull();

				} )
				.handle( (r, x) -> {
					assertThat( x.getCause() ).isInstanceOf( NoResultException.class );
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
						s.createSelectionQuery( "from Author", Author.class ).getSingleResult()
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
						s.createSelectionQuery( "from Author", Author.class ).getSingleResultOrNull()
				) )
		);
	}

	@Test
	public void testSelectionQueryGetResultCountWithStage(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( author1, author2 ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createSelectionQuery( "from Author", Author.class )
						.getResultCount() ) )
				.thenAccept( count -> assertThat( count ).isEqualTo( 2L ) )
		);
	}

	@Test
	public void testQueryGetResultCountWithStage(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( author1, author2 ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createQuery( "from Author", Author.class )
						.getResultCount() ) )
				.thenAccept( count -> assertThat( count ).isEqualTo( 2L ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createQuery( "from Author", Author.class )
						.setMaxResults( 1 )
						.setFirstResult( 1 )
						.getResultCount() ) )
				.thenAccept( count -> assertThat( count ).isEqualTo( 2L ) )
		);
	}

	@Test
	public void testSelectionQueryGetResultCountWithMutiny(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( author1, author2 ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createSelectionQuery( "from Author", Author.class )
						.getResultCount() ) )
				.thenAccept( count -> assertThat( count ).isEqualTo( 2L ) )
		);
	}

	@Test
	public void testQueryGetResultCountWithMutiny(VertxTestContext context) {
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( author1, author2 ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s
						.createQuery( "from Author", Author.class )
						.getResultCount() ) )
				.invoke( count -> assertThat( count ).isEqualTo( 2L ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s
						.createQuery( "from Author", Author.class )
						.setMaxResults( 1 )
						.setFirstResult( 1 )
						.getResultCount() ) )
				.invoke( count -> assertThat( count ).isEqualTo( 2L ) )
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

		@Override
		public String toString() {
			return id + ":" + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Author author = (Author) o;
			return Objects.equals( name, author.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
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

		BigDecimal quantity;

		Book(String isbn, String title, Author author) {
			this.title = title;
			this.isbn = isbn;
			this.author = author;
		}

		Book() {
		}

		@Override
		public String toString() {
			return id + ":" + title + ":" + isbn + ":" + quantity;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Book book = (Book) o;
			return Objects.equals( isbn, book.isbn ) && Objects.equals(
					title,
					book.title
			);
		}

		@Override
		public int hashCode() {
			return Objects.hash( isbn, title );
		}
	}

}

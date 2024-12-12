/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.type.SqlTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(POSTGRESQL)
public class JsonQueryTest extends BaseReactiveTest {

	private final static BigDecimal PIE = BigDecimal.valueOf( 3.1416 );
	private final static BigDecimal TAO = BigDecimal.valueOf( 6.2832 );

	final Book fakeHistory = new Book( 3, "Fake History", new JsonObject().put( "amount", PIE ), new Book.Author( "Jo", "Hedwig Teeuwisse" ) );
	final Book theBookOfM = new Book( 5, "The Book of M", new JsonObject().put( "amount", TAO ), new Book.Author( "Peng", "Shepherd" ) );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( s -> s.persistAll( theBookOfM, fakeHistory ) ) );
	}

	@Test
	public void criteriaSelectAll(VertxTestContext context) {
		CriteriaBuilder cb = getMutinySessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> bookQuery = cb.createQuery( Book.class );
		bookQuery.from( Book.class );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.createQuery( bookQuery ).getResultList() )
				.invoke( results -> assertThat( results ).containsExactlyInAnyOrder( fakeHistory, theBookOfM ) )
		);
	}

	@Test
	public void criteriaQueryWithJsonbAndFunction(VertxTestContext context) {
		CriteriaBuilder cb = getMutinySessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> bookQuery = cb.createQuery( Book.class );
		Root<Book> bookRoot = bookQuery.from( Book.class );
		bookQuery.where( cb.equal(
				cb.function( "jsonb_extract_path_text", String.class, bookRoot.get( "author" ), cb.literal( "name" ) ),
				cb.literal( fakeHistory.author.name )
		) );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.createQuery( bookQuery ).getSingleResult() )
				.invoke( result -> assertThat( result ).isEqualTo( fakeHistory ) )
		);
	}

	@Test
	public void criteriaQueryWithJson(VertxTestContext context) {
		CriteriaBuilder cb = getMutinySessionFactory().getCriteriaBuilder();
		CriteriaQuery<Book> bookQuery = cb.createQuery( Book.class );
		bookQuery.from( Book.class );
		bookQuery.where( cb.between(
				cb.function( "sql", BigDecimal.class, cb.literal( "(price ->> ?)::decimal" ), cb.literal( "amount" ) ),
				BigDecimal.valueOf( 4.0 ),
				BigDecimal.valueOf( 100.0 )
		) );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.createQuery( bookQuery ).getSingleResult() )
				.invoke( result -> assertThat( result ).isEqualTo( theBookOfM ) )
		);
	}

	@Test
	public void hqlQueryWithJson(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createSelectionQuery(
								"from Book where sql('(price ->> ?)::decimal', 'amount') between ?1 and ?2",
								Book.class
						)
						.setParameter( 1, BigDecimal.valueOf( 4.0 ) )
						.setParameter( 2, BigDecimal.valueOf( 100.0 ) )
						.getSingleResult()
				)
				.invoke( result -> assertThat( result ).isEqualTo( theBookOfM ) )
		);
	}

	@Disabled("https://github.com/hibernate/hibernate-reactive/issues/1999")
	@Test
	public void nativeSelectAll(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.createNativeQuery( "select * from BookWithJson", Book.class ).getResultList() )
				.invoke( results -> assertThat( results ).containsExactlyInAnyOrder( fakeHistory, theBookOfM ) )
		);
	}

	@Disabled("https://github.com/hibernate/hibernate-reactive/issues/1999")
	@Test
	public void nativeSelectWithoutResultType(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.createNativeQuery( "select * from BookWithJson" ).getResultList() )
				.invoke( results -> assertThat( results ).containsExactlyInAnyOrder( fakeHistory, theBookOfM ) )
		);
	}

	@Disabled("https://github.com/hibernate/hibernate-reactive/issues/1999")
	@Test
	public void nativeQueryWithJson(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createNativeQuery(
								"select * from BookWithJson b where (b.price ->> 'amount')::decimal between ?1 and ?2",
								Book.class
						)
						.setParameter( 1, BigDecimal.valueOf( 4.0 ) )
						.setParameter( 2, BigDecimal.valueOf( 100.0 ) )
						.getSingleResult()
				)
				.invoke( result -> assertThat( result ).isEqualTo( theBookOfM ) )
		);
	}

	@Test
	@Disabled("https://github.com/hibernate/hibernate-reactive/issues/1999")
	public void nativeQueryWithEscapedQuestionMark(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createNativeQuery( "select * from BookWithJson where author -> 'name' \\? 'Jo'", Book.class )
						.getSingleResult()
				)
				.invoke( result -> assertThat( result ).isEqualTo( fakeHistory ) )
		);
	}

	@Test
	@Disabled("https://github.com/hibernate/hibernate-reactive/issues/2012")
	public void nativeQuerySelectScalarWithEscapedQuestionMark(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createNativeQuery( "select 123 from BookWithJson where author -> 'name' \\? 'Jo'", Object.class )
						.getSingleResult()
				)
				.invoke( result -> assertThat( result ).isEqualTo( 123 ) )
		);
	}

	@Entity(name = "Book")
	@Table(name = "BookWithJson")
	public static class Book {

		@Id
		Integer id;

		String title;

		@Column(name = "price")
		JsonObject price;

		@JdbcTypeCode(SqlTypes.JSON)
		Author author;

		public Book() {
		}

		public Book(Integer id, String title, JsonObject price, Author author) {
			this.id = id;
			this.title = title;
			this.price = price;
			this.author = author;
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
			return Objects.equals( id, book.id ) && Objects.equals(
					title,
					book.title
			) && Objects.equals( price, book.price ) && Objects.equals( author, book.author );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, title, price, author );
		}

		@Override
		public String toString() {
			return id + ":" + title + ":" + price + ":" + author;
		}


		@Embeddable
		public static class Author {
			private String name;
			private String surname;

			public Author() {
			}

			public Author(String name, String surname) {
				this.name = name;
				this.surname = surname;
			}

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

			public String getSurname() {
				return surname;
			}

			public void setSurname(String surname) {
				this.surname = surname;
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
				return Objects.equals( name, author.name ) && Objects.equals( surname, author.surname );
			}

			@Override
			public int hashCode() {
				return Objects.hash( name, surname );
			}

			@Override
			public String toString() {
				return name + ' ' + surname;
			}
		}
	}
}

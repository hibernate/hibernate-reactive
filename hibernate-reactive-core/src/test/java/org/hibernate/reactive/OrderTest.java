/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.metamodel.model.domain.EntityDomainType;
import org.hibernate.metamodel.model.domain.internal.MappingMetamodelImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.metamodel.SingularAttribute;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.query.Order.asc;
import static org.hibernate.query.Order.desc;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 10, timeUnit = MINUTES)
public class OrderTest extends BaseReactiveTest {
	final Book book1 = new Book("9781932394153", "Hibernate in Action");
	final Book book2 = new Book("9781617290459", "Java Persistence with Hibernate");

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( book1, book2 ) )
		);
	}

	@Test
	public void testOrder(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> isbn = getIsbnAttribute();
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getSessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book", Book.class )
						.setOrder( asc( title ) ).getResultList()
						.thenAccept( books -> assertThat( books ).contains( book1, book2 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( desc( title ) ).getResultList()
						).thenAccept( books -> assertThat( books ).containsExactly( book2, book1 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( asc( isbn ) )
								.getResultList()
						).thenAccept( books -> assertThat( books ).containsExactly( book2, book1 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( desc( isbn ) )
								.getResultList()
						).thenAccept( books -> assertThat( books ).containsExactly( book1, book2 ) )
				)
		);
	}

	@Test
	public void testOrderMutiny(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> isbn = getIsbnAttribute();
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getMutinySessionFactory().withSession( session -> session
						.createSelectionQuery( "select title from Book", String.class )
						.getResultList()
						.invoke( list -> assertEquals( 2, list.size() ) )
						.chain( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( asc( title ) )
								.getResultList() ).invoke( books ->  assertThat( books ).contains( book1, book2 ) )
						.chain( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( desc( title ) )
								.getResultList() ).invoke( books -> assertThat( books ).containsExactly( book2, book1 ) )
						.chain( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( asc( isbn ) )
								.getResultList() ).invoke( books -> assertThat( books ).containsExactly( book2, book1 ) )
						.chain( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( desc( isbn ) )
								.getResultList() ).invoke( books -> assertThat( books ).contains( book1, book2 )  )
				)
		);
	}

	@Test
	public void testAscDescBySelectElement(VertxTestContext context) {
		test(
				context,
				getSessionFactory().withSession( session -> session
						.createSelectionQuery( "select isbn, title from Book", Object[].class )
						.setOrder( asc( 2 ) ).getResultList()
						.thenAccept( list -> assertOrderByBookArray( list, book1, book2 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "select isbn, title from Book", Object[].class )
								.setOrder( desc( 2 ) ).getResultList()
								.thenAccept( list -> assertOrderByBookArray( list, book2, book1 ) )
						)
				)
		);
	}

	@Test
	public void testAscDescBySelectElementMutiny(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory().withSession( session -> session
						.createSelectionQuery( "select isbn, title from Book", Object[].class )
						.setOrder( asc( 2 ) ).getResultList()
						.invoke( list ->  assertOrderByBookArray( list, book1, book2 ) )
						.chain( v -> session
								.createSelectionQuery( "select isbn, title from Book", Object[].class )
								.setOrder( desc( 2 ) )
								.getResultList() ).invoke( list ->  assertOrderByBookArray( list, book2, book1 ) )
				)
		);
	}

	@Test
	public void testOrderWithList(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> isbn = getIsbnAttribute();
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getSessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book", Book.class )
						.setOrder( List.of( asc( isbn ), desc( title ) ) ).getResultList()
						.thenAccept( isbnAsc -> assertThat( isbnAsc ).containsExactly( book2, book1 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( List.of( desc( isbn ), desc( title ) ) ).getResultList()
								.thenAccept( isbnDesc -> assertThat( isbnDesc ).containsExactly( book1, book2 ) )
						)
				)
		);
	}

	@Test
	public void testOrderWithListMutiny(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> isbn = getIsbnAttribute();
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getMutinySessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book", Book.class )
						.setOrder( List.of( asc( isbn ), desc( title ) ) ).getResultList()
						.invoke( isbnAsc -> assertThat( isbnAsc ).containsExactly( book2, book1 ) )
						.chain( v -> session
								.createSelectionQuery( "from Book", Book.class )
								.setOrder( List.of( desc( isbn ), desc( title ) ) ).getResultList()
								.invoke( isbnDesc -> assertThat( isbnDesc ).containsExactly( book1, book2 ) )
						)
				)
		);
	}

	@Test
	public void testAscDescWithNamedParam(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getSessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( asc( title ) )
						.getResultList()
						.thenAccept( list -> assertOrderByBook( list, book1, book2 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "from Book where title like :title", Book.class )
								.setParameter( "title", "%Hibernate%" )
								.setOrder( desc( title ) ).getResultList()
								.thenAccept( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
						)
				)
		);
	}

	@Test
	public void testAscDescWithNamedParamMutiny(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getMutinySessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( asc( title ) )
						.getResultList()
						.invoke( list -> assertOrderByBook( list, book1, book2 ) )
						.chain( v -> session
								.createSelectionQuery( "from Book where title like :title", Book.class )
								.setParameter( "title", "%Hibernate%" )
								.setOrder( desc( title ) ).getResultList()
								.invoke( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
						)
				)
		);
	}

	@Test
	public void testAscDescWithPositionalParam(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getSessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( asc( title ) )
						.getResultList()
						.thenAccept( list -> assertOrderByBook( list, book1, book2 ) )
						.thenCompose( v -> session
								.createSelectionQuery( "from Book where title like :title", Book.class )
								.setParameter( "title", "%Hibernate%" )
								.setOrder( desc( title ) ).getResultList()
								.thenAccept( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
						)
				)
		);
	}

	@Test
	public void testAscDescWithPositionalParamMutiny(VertxTestContext context) {
		final SingularAttribute<? super Book, ?> title = getTitleAttribute();

		test(
				context,
				getMutinySessionFactory().withSession( session -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( asc( title ) )
						.getResultList()
						.invoke( list -> assertOrderByBook( list, book1, book2 ) )
						.chain( v -> session
								.createSelectionQuery( "from Book where title like :title", Book.class )
								.setParameter( "title", "%Hibernate%" )
								.setOrder( desc( title ) ).getResultList()
								.invoke( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
						)
				)
		);
	}

	private void assertOrderByBookArray(List<Object[]> resultList, Book first, Book second ) {
		List<?> titles = resultList.stream().map( book -> (book[1]) ).collect( toList() );
		assertEquals( first.title, titles.get( 0 ) );
		assertEquals( second.title, titles.get( 1 ) );
	}

	private void assertOrderByBook(List<Book> resultList, Book first, Book second ) {
		List<?> titles = resultList.stream().map( book -> book.title ).collect( toList() );
		assertEquals( first.title, titles.get( 0 ) );
		assertEquals( second.title, titles.get( 1 ) );
	}

	private SingularAttribute<? super Book, ?> getIsbnAttribute() {
		MappingMetamodelImpl metamodel = (MappingMetamodelImpl) getSessionFactory().getMetamodel();
		EntityDomainType<Book> bookType = metamodel.getJpaMetamodel().findEntityType( Book.class );
		return bookType.findSingularAttribute( "isbn" );
	}

	private SingularAttribute<? super Book, ?> getTitleAttribute() {
		MappingMetamodelImpl metamodel = (MappingMetamodelImpl) getSessionFactory().getMetamodel();
		EntityDomainType<Book> bookType = metamodel.getJpaMetamodel().findEntityType( Book.class );
		return bookType.findSingularAttribute( "title" );
	}

	@Entity(name="Book")
	static class Book {
		@Id
		String isbn;
		String title;

		Book(String isbn, String title) {
			this.isbn = isbn;
			this.title = title;
		}

		Book() {
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Book book = (Book)o;
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

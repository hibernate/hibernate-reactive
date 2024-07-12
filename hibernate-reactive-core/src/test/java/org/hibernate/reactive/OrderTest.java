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
import jakarta.persistence.Table;
import jakarta.persistence.metamodel.SingularAttribute;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.query.Order.asc;
import static org.hibernate.query.Order.desc;

@Timeout(value = 10, timeUnit = MINUTES)
public class OrderTest extends BaseReactiveTest {
	final Book book1 = new Book( "9781932394153", "Hibernate in Action" );
	final Book book2 = new Book( "9781617290459", "Java Persistence with Hibernate" );

	SingularAttribute<? super Book, ?> isbn;
	SingularAttribute<? super Book, ?> title;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		isbn = attribute( "isbn" );
		title = attribute( "title" );
		test( context, getSessionFactory().withTransaction( session -> session.persist( book1, book2 ) ) );
	}

	@Test
	public void descPositionalColumnWithStage(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				// Not sure if it's a bug, but setOrder doesn't work if we use String.class
				.createSelectionQuery( "select title from Book", Object[].class )
				.setOrder( desc( 1 ) )
				.getResultList()
				.thenAccept( results -> assertThat( results )
						// Keep the title
						.map( row -> row[0] )
						.containsExactly( book2.title, book1.title ) )
		) );
	}

	@Test
	public void descPositionalColumnWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				// Not sure if it's a bug, but setOrder doesn't work if we use String.class or Object.class
				.createSelectionQuery( "select title from Book", Object[].class )
				.setOrder( desc( 1 ) )
				.getResultList()
				.invoke( results -> assertThat( results )
						// Keep the title
						.map( row -> row[0] )
						.containsExactly( book2.title, book1.title )
				)
		) );
	}

	@Test
	public void ascAttributeWithStage(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( asc( title ) )
				.getResultList()
				.thenAccept( books -> assertThat( books ).containsExactly( book1, book2 ) )
		) );
	}

	@Test
	public void ascAttributeWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( asc( title ) )
				.getResultList()
				.invoke( books -> assertThat( books ).containsExactly( book1, book2 ) )
		) );
	}

	@Test
	public void descAttributeWithStage(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( desc( title ) )
				.getResultList()
				.thenAccept( books -> assertThat( books ).containsExactly( book2, book1 ) )
		) );
	}

	@Test
	public void descAttributeWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( desc( title ) )
				.getResultList()
				.invoke( books -> assertThat( books ).containsExactly( book2, book1 ) )
		) );
	}

	@Test
	public void ascIdWithStage(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( asc( isbn ) )
				.getResultList()
				.thenAccept( books -> assertThat( books ).containsExactly( book2, book1 ) )
		) );
	}

	@Test
	public void ascIdWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( asc( isbn ) )
				.getResultList()
				.invoke( books -> assertThat( books ).containsExactly( book2, book1 ) )
		) );
	}

	@Test
	public void descIdWithStage(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( desc( isbn ) )
				.getResultList()
				.thenAccept( books -> assertThat( books ).containsExactly( book1, book2 ) )
		) );
	}

	@Test
	public void descIdWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( desc( isbn ) )
				.getResultList()
				.invoke( books -> assertThat( books ).containsExactly( book1, book2 ) )
		) );
	}

	@Test
	public void testAscDescBySelectElement(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "select isbn, title from Book", Object[].class )
				.setOrder( asc( 2 ) )
				.getResultList()
				.thenAccept( list -> assertOrderByBookArray( list, book1, book2 ) )
				.thenCompose( v -> session
						.createSelectionQuery( "select isbn, title from Book", Object[].class )
						.setOrder( desc( 2 ) )
						.getResultList()
						.thenAccept( list -> assertOrderByBookArray( list, book2, book1 ) )
				)
		) );
	}

	@Test
	public void testAscDescBySelectElementMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "select isbn, title from Book", Object[].class )
				.setOrder( asc( 2 ) )
				.getResultList()
				.invoke( list -> assertOrderByBookArray( list, book1, book2 ) )
				.chain( v -> session
						.createSelectionQuery( "select isbn, title from Book", Object[].class )
						.setOrder( desc( 2 ) )
						.getResultList()
						.invoke( list -> assertOrderByBookArray( list, book2, book1 ) )
				)
		) );
	}

	@Test
	public void testOrderWithList(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( List.of( asc( isbn ), desc( title ) ) ).getResultList()
				.thenAccept( isbnAsc -> assertThat( isbnAsc ).containsExactly( book2, book1 ) )
				.thenCompose( v -> session
						.createSelectionQuery( "from Book", Book.class )
						.setOrder( List.of( desc( isbn ), desc( title ) ) ).getResultList()
						.thenAccept( isbnDesc -> assertThat( isbnDesc ).containsExactly( book1, book2 ) )
				)
		) );
	}

	@Test
	public void testOrderWithListMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book", Book.class )
				.setOrder( List.of( asc( isbn ), desc( title ) ) ).getResultList()
				.invoke( isbnAsc -> assertThat( isbnAsc ).containsExactly( book2, book1 ) )
				.chain( v -> session
						.createSelectionQuery( "from Book", Book.class )
						.setOrder( List.of( desc( isbn ), desc( title ) ) )
						.getResultList()
						.invoke( isbnDesc -> assertThat( isbnDesc ).containsExactly( book1, book2 ) )
				)
		) );
	}

	@Test
	public void testAscDescWithNamedParam(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book where title like :title", Book.class )
				.setParameter( "title", "%Hibernate%" )
				.setOrder( asc( title ) )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( book1, book2 ) )
				.thenCompose( v -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( desc( title ) ).getResultList()
						.thenAccept( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
				)
		) );
	}

	@Test
	public void testAscDescWithNamedParamMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book where title like :title", Book.class )
				.setParameter( "title", "%Hibernate%" )
				.setOrder( asc( title ) )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( book1, book2 ) )
				.chain( v -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( desc( title ) )
						.getResultList()
						.invoke( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
				)
		) );
	}

	@Test
	public void testAscDescWithPositionalParam(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book where title like :title", Book.class )
				.setParameter( "title", "%Hibernate%" )
				.setOrder( asc( title ) )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( book1, book2 ) )
				.thenCompose( v -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( desc( title ) )
						.getResultList()
						.thenAccept( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
				)
		) );
	}

	@Test
	public void testAscDescWithPositionalParamMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
				.createSelectionQuery( "from Book where title like :title", Book.class )
				.setParameter( "title", "%Hibernate%" )
				.setOrder( asc( title ) )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( book1, book2 ) )
				.chain( v -> session
						.createSelectionQuery( "from Book where title like :title", Book.class )
						.setParameter( "title", "%Hibernate%" )
						.setOrder( desc( title ) )
						.getResultList()
						.invoke( isbnDesc -> assertThat( isbnDesc ).containsExactly( book2, book1 ) )
				)
		) );
	}

	private void assertOrderByBookArray(List<Object[]> resultList, Book... expectedBooks) {
		List<Book> books = resultList.stream()
				.map( objects -> new Book( (String) objects[0], (String) objects[1] ) )
				.collect( toList() );
		assertThat( books ).containsExactly( expectedBooks );
	}

	private SingularAttribute<? super Book, ?> attribute(String name) {
		MappingMetamodelImpl metamodel = (MappingMetamodelImpl) getSessionFactory().getMetamodel();
		EntityDomainType<Book> bookType = metamodel.getJpaMetamodel().findEntityType( Book.class );
		return bookType.findSingularAttribute( name );
	}

	@Entity(name = "Book")
	@Table(name = "OrderTest_Book" )
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

		@Override
		public String toString() {
			return isbn + ":" + title;
		}
	}
}

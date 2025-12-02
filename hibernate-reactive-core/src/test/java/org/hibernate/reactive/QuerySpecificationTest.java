/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.metamodel.model.domain.EntityDomainType;
import org.hibernate.metamodel.model.domain.internal.MappingMetamodelImpl;
import org.hibernate.query.Order;
import org.hibernate.query.restriction.Restriction;
import org.hibernate.query.specification.MutationSpecification;
import org.hibernate.query.specification.SelectionSpecification;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.metamodel.SingularAttribute;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.query.Order.asc;
import static org.hibernate.query.Order.desc;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test for queries created using {@link SelectionSpecification}.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class QuerySpecificationTest extends BaseReactiveTest {
	static final Book hibBook = new Book( 1L, "Hibernate in Action" );
	static final Book jpBook = new Book( 3L, "Java Persistence with Hibernate" );
	static final Book animalFarmBook = new Book( 2L, "Animal Farm" );

	// This is only added when testing order with multiple columns
	static final Book animalFarmBook2 = new Book( 4L, "Animal Farm" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( session -> session
				.persist( hibBook, jpBook, animalFarmBook ) )
		);
	}

	static Stream<Arguments> singleColumnOrderExpectation() {
		return Stream.of(
				arguments( asc( Book.class, "title" ), List.of( animalFarmBook, hibBook, jpBook ) ),
				arguments( desc( Book.class, "title" ), List.of( jpBook, hibBook, animalFarmBook ) ),
				arguments( asc( Book.class, "isbn" ), List.of( hibBook, animalFarmBook, jpBook ) ),
				arguments( desc( Book.class, "isbn" ), List.of( jpBook, animalFarmBook, hibBook ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleColumnOrderWithStage(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( order )
				.reference();

		test( context, getSessionFactory()
				.withSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.thenAccept( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleColumnOrderWithStageStateless(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( order )
				.reference();

		test( context, getSessionFactory()
				.withStatelessSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.thenAccept( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleColumnOrderWithMutiny(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( order )
				.reference();

		test( context, getMutinySessionFactory()
				.withSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.invoke( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleColumnOrderWithMutinyStateless(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( order )
				.reference();

		test( context, getMutinySessionFactory()
				.withStatelessSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.invoke( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleAttributeOrderWithStage(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		final SingularAttribute<? super Book, ?> attribute = attribute( order.attributeName() );
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( Order.by( attribute, order.direction() ) )
				.reference();

		test( context, getSessionFactory()
				.withSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.thenAccept( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleAttributeOrderWithStageStateless(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		final SingularAttribute<? super Book, ?> attribute = attribute( order.attributeName() );
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( Order.by( attribute, order.direction() ) )
				.reference();

		test( context, getSessionFactory()
				.withStatelessSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.thenAccept( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleAttributeOrderWithMutiny(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		final SingularAttribute<? super Book, ?> attribute = attribute( order.attributeName() );
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( Order.by( attribute, order.direction() ) )
				.reference();

		test( context, getMutinySessionFactory()
				.withSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.invoke( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("singleColumnOrderExpectation")
	public void singleAttributeOrderWithMutinyStateless(Order<Book> order, List<Book> expectedList, VertxTestContext context) {
		final SingularAttribute<? super Book, ?> attribute = attribute( order.attributeName() );
		var bookReference = SelectionSpecification
				.create( Book.class, "from Book" )
				.sort( Order.by( attribute, order.direction() ) )
				.reference();

		test( context, getMutinySessionFactory()
				.withStatelessSession( session -> session
						.createQuery( bookReference )
						.getResultList() )
				.invoke( books -> assertThat( books ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("multipleColumnOrderExpectation")
	public void multipleColumnsOrderWithStage(List<Order<Book>> orders, List<Book> expectedList, VertxTestContext context) {
		var columnsSpec = SelectionSpecification
				.create( Book.class, "from Book" );
		for ( Order<Book> order : orders ) {
			columnsSpec.sort( order );
		}

		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.createQuery( columnsSpec.reference() )
						.getResultList() ) )
				.thenAccept( list -> assertThat( list ).isEqualTo( expectedList ) )
		);
	}

	static Stream<Arguments> multipleColumnOrderExpectation() {
		return Stream.of(
				arguments(
						List.of( asc( Book.class, "title" ), asc( Book.class, "isbn" ) ),
						List.of( animalFarmBook, animalFarmBook2, hibBook, jpBook )
				),
				arguments(
						List.of( asc( Book.class, "title" ), desc( Book.class, "isbn" ) ),
						List.of( animalFarmBook2, animalFarmBook, hibBook, jpBook )
				),
				arguments(
						List.of( desc( Book.class, "isbn" ), asc( Book.class, "title" ) ),
						List.of( animalFarmBook2, jpBook, animalFarmBook, hibBook )
				),
				arguments(
						List.of( desc( Book.class, "isbn" ), desc( Book.class, "title" ) ),
						List.of( animalFarmBook2, jpBook, animalFarmBook, hibBook )
				)
		);
	}

	@ParameterizedTest
	@MethodSource("multipleColumnOrderExpectation")
	public void multipleColumnsOrderWithStageStateless(List<Order<Book>> orders, List<Book> expectedList, VertxTestContext context) {
		var columnsSpec = SelectionSpecification
				.create( Book.class, "from Book" );
		for ( Order<Book> order : orders ) {
			columnsSpec.sort( order );
		}

		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( session -> session
						.createQuery( columnsSpec.reference() )
						.getResultList() ) )
				.thenAccept( list -> assertThat( list ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("multipleColumnOrderExpectation")
	public void multipleColumnsOrderWithMutiny(List<Order<Book>> orders, List<Book> expectedList, VertxTestContext context) {
		var columnsSpec = SelectionSpecification
				.create( Book.class, "from Book" );
		for ( Order<Book> order : orders ) {
			columnsSpec.sort( order );
		}

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.chain( v -> getMutinySessionFactory().withSession( session -> session
						.createQuery( columnsSpec.reference() )
						.getResultList() ) )
				.invoke( list -> assertThat( list ).isEqualTo( expectedList ) )
		);
	}

	@ParameterizedTest
	@MethodSource("multipleColumnOrderExpectation")
	public void multipleColumnsOrderWithMutinyStateless(List<Order<Book>> orders, List<Book> expectedList, VertxTestContext context) {
		var columnsSpec = SelectionSpecification
				.create( Book.class, "from Book" );
		for ( Order<Book> order : orders ) {
			columnsSpec.sort( order );
		}

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.chain( v -> getMutinySessionFactory().withStatelessSession( session -> session
						.createQuery( columnsSpec.reference() )
						.getResultList() ) )
				.invoke( list -> assertThat( list ).isEqualTo( expectedList ) )
		);
	}

	@Test
	public void mutationSpecificationWithStage(VertxTestContext context) {
		SingularAttribute<Book, String> title = (SingularAttribute<Book, String>) attribute( "title" );
		TypedQueryReference<Void> deleteAnimalFarm = MutationSpecification
				.create( Book.class, "delete Book" )
				.restrict( Restriction.equalIgnoringCase( title, animalFarmBook.title ) )
				.reference();

		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
						.createQuery( deleteAnimalFarm )
						.executeUpdate() ) )
				.thenAccept( deleted -> assertThat( deleted ).isEqualTo( 2 ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.createQuery( "from Book", Book.class ).getResultList() ) )
				.thenAccept( list -> assertThat( list ).containsExactlyInAnyOrder( hibBook, jpBook ) )
		);
	}

	@Test
	public void mutationSpecificationWithStageStateless(VertxTestContext context) {
		SingularAttribute<Book, String> title = (SingularAttribute<Book, String>) attribute( "title" );
		TypedQueryReference<Void> deleteAnimalFarm = MutationSpecification
				.create( Book.class, "delete Book" )
				.restrict( Restriction.equalIgnoringCase( title, animalFarmBook.title ) )
				.reference();

		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( session -> session
						.createQuery( deleteAnimalFarm )
						.executeUpdate() ) )
				.thenAccept( deleted -> assertThat( deleted ).isEqualTo( 2 ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.createQuery( "from Book", Book.class ).getResultList() ) )
				.thenAccept( list -> assertThat( list ).containsExactlyInAnyOrder( hibBook, jpBook ) )
		);
	}

	@Test
	public void mutationSpecificationWithMutiny(VertxTestContext context) {
		SingularAttribute<Book, String> title = (SingularAttribute<Book, String>) attribute( "title" );
		TypedQueryReference<Void> deleteAnimalFarm = MutationSpecification
				.create( Book.class, "delete Book" )
				.restrict( Restriction.equalIgnoringCase( title, animalFarmBook.title ) )
				.reference();

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( animalFarmBook2 ) )
				.chain( v -> getMutinySessionFactory().withTransaction( session -> session
						.createQuery( deleteAnimalFarm )
						.executeUpdate() ) )
				.invoke( deleted -> assertThat( deleted ).isEqualTo( 2 ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.createQuery( "from Book", Book.class ).getResultList() ) )
				.invoke( list -> assertThat( list ).containsExactlyInAnyOrder( hibBook, jpBook ) )
		);
	}

	@Test
	public void mutationSpecificationWithMutinyStateless(VertxTestContext context) {
		SingularAttribute<Book, String> title = (SingularAttribute<Book, String>) attribute( "title" );
		TypedQueryReference<Void> deleteAnimalFarm = MutationSpecification
				.create( Book.class, "delete Book" )
				.restrict( Restriction.equalIgnoringCase( title, animalFarmBook.title ) )
				.reference();

		test( context, getMutinySessionFactory()
				.withStatelessTransaction( session -> session.insert( animalFarmBook2 ) )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( session -> session
						.createQuery( deleteAnimalFarm )
						.executeUpdate() ) )
				.invoke( deleted -> assertThat( deleted ).isEqualTo( 2 ) )
				.chain( () -> getMutinySessionFactory().withStatelessSession( session -> session
						.createQuery( "from Book", Book.class ).getResultList() ) )
				.invoke( list -> assertThat( list ).containsExactlyInAnyOrder( hibBook, jpBook ) )
		);
	}

	private SingularAttribute<? super Book, ?> attribute(String name) {
		MappingMetamodelImpl metamodel = (MappingMetamodelImpl) getSessionFactory().getMetamodel();
		EntityDomainType<Book> bookType = metamodel.getJpaMetamodel().findEntityType( Book.class );
		return bookType.findSingularAttribute( name );
	}

	@Entity(name = "Book")
	@Table(name = "OrderTest_Book" )
	public static class Book {
		@Id
		Long isbn;
		String title;

		Book(Long isbn, String title) {
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

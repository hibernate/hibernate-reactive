/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests @{@link ElementCollection} on a {@link List} of basic types.
 * <p>
 * Example:
 * {@code
 *     class Person {
 *         @ElementCollection
 *         List<String> phones;
 *     }
 * }
 * </p>
 *
 * @see EagerElementCollectionForBasicTypeSetTest
 * @see EagerElementCollectionForEmbeddableTypeListTest
 */
public class EagerElementCollectionForBasicTypeListTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		List<String> phones = Arrays.asList( "999-999-9999", "111-111-1111", "123-456-7890" );
		thePerson = new Person( 7242000, "Claude", phones );

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) ) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		Person johnny = new Person( 999, "Johnny English", Arrays.asList( "888", "555" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( johnny ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, johnny.getId() ) )
				.invoke( found -> assertPhones( found, "888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session .find( Person.class, thePerson.getId() ) )
				.thenAccept( found -> assertPhones( found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithMutinyAPI(VertxTestContext context) {
		test( context, openMutinySession()
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( found -> assertPhones( found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		Person thomas = new Person( 7, "Thomas Reaper", Arrays.asList( "111", "111", "111", "111" ) );

		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		Person thomas = new Person( 567, "Thomas Reaper", Arrays.asList( "111", "111", "111", "111" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.thenCompose( v -> getSessionFactory().withTransaction( (s, t) -> s
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.thenAccept( found -> {
							found.getPhones().set( 1, "47" );
							found.getPhones().set( 3, "47" );
						} ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( found, "000", "47", "000", "47" ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session
								.find( Person.class, thomas.getId() )
								// Change a couple of the elements in the collection
								.invoke( found -> {
									found.getPhones().set( 1, "47" );
									found.getPhones().set( 3, "47" );
								} ) )
				)
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( found, "000", "47", "000", "47" ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( Person.class, thomas.getId() )
								// Change one of the element in the collection
								.thenAccept( found -> {
									// it doesn't matter which elements are deleted because they are all equal
									found.getPhones().remove( 1 );
									found.getPhones().remove( 2 );
								} )
						)
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( found, "000", "000" ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.find( Person.class, thomas.getId() )
								// Change one of the element in the collection
								.invoke( found -> {
									// it doesn't matter which elements are deleted because they are all equal
									found.getPhones().remove( 1 );
									found.getPhones().remove( 2 );
								} )
						)
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session.find( Person.class, thomas.getId() ) ) )
				.invoke( found -> assertPhones( found, "000", "000" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add( "000" ) ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( updatedPerson -> assertPhones( updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( "000" ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.invoke( updatedPerson -> assertPhones( updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( updatedPerson -> assertPhones( updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.invoke( updatedPerson -> assertPhones( updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(VertxTestContext context){
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( changedPerson -> assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.invoke( changedPerson -> assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(VertxTestContext context){
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( changedPerson -> assertPhones( changedPerson, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} )
				)
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( person -> assertPhones( person, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( List.of( "555" ) );
						} )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( changedPerson, "555" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertNotNull( foundPerson );
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( List.of( "555" ) );
						} )
				)
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( changedPerson, "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( Assertions::assertNull )
				// Check with native query that the table is empty
				.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
				.thenAccept( resultList -> assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void removePersonWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.call( session::remove )
				)
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( Assertions::assertNull )
				// Check with native query that the table is empty
				.chain( () -> selectFromPhonesWithMutiny( thePerson ) )
				.invoke( resultList -> assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( foundPerson, "222-222-2222", "333-333-3333" ) )
				// Check initial person collection hasn't changed
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				.invoke( foundPerson -> assertPhones( foundPerson, "222-222-2222", "333-333-3333" ) )
				// Check initial person collection hasn't changed
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertPhones( foundPerson, "567" ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertPhones( foundPerson, "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( EagerElementCollectionForBasicTypeListTest::assertPhones )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.find( Person.class, thePerson.getId() )
						.invoke( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
				)
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( EagerElementCollectionForBasicTypeListTest::assertPhones )
		);
	}

	/**
	 * With the Stage API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person.
	 */
	private CompletionStage<List<Object>> selectFromPhonesWithStage(Person person) {
		return openSession().thenCompose( session -> session
				.createNativeQuery( "SELECT * FROM Person_phones where Person_id = ?" )
				.setParameter( 1, person.getId() )
				.getResultList() );
	}

	/**
	 * With the Mutiny API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person
	 */
	private Uni<List<Object>> selectFromPhonesWithMutiny(Person person) {
		return openMutinySession().chain( session -> session
				.createNativeQuery( "SELECT * FROM Person_phones where Person_id = ?" )
				.setParameter( 1, person.getId() )
				.getResultList() );
	}

	private static void assertPhones(Person person, String... expectedPhones) {
		assertNotNull( person );
		assertThat( person.getPhones() ).containsExactlyInAnyOrder( expectedPhones );
	}

	@Entity(name = "Person")
	@Table(name = "Person")
	static class Person {
		@Id
		private Integer id;
		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		private List<String> phones;

		public Person() {
		}

		public Person(Integer id, String name, List<String> phones) {
			this.id = id;
			this.name = name;
			this.phones = phones;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setPhones(List<String> phones) {
			this.phones = phones;
		}

		public List<String> getPhones() {
			return phones;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals( name, person.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + phones;
		}
	}
}

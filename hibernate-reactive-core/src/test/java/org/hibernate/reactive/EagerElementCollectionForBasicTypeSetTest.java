/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests @{@link ElementCollection} on a {@link Set} of basic types.
 * <p>
 * Example:
 * {@code
 *     class Person {
 *         @ElementCollection
 *         Set<String> phones;
 *     }
 * }
 * </p>,
 *
 * @see EagerElementCollectionForBasicTypeListTest
 * @see EagerElementCollectionForEmbeddableTypeListTest
 */
@Timeout( value = 5, timeUnit = TimeUnit.MINUTES )
public class EagerElementCollectionForBasicTypeSetTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	private Person getPerson() {
		if( thePerson == null ) {
			Set<String> phones = new HashSet<>( Arrays.asList( "999-999-9999", "111-111-1111", "123-456-7890" ) );
			thePerson = new Person( 7242000, "Claude", phones );
		}
		return thePerson;
	}

	private Uni<Void> populateDbMutiny() {
		return getMutinySessionFactory().withTransaction( (s, t) -> s.persist( getPerson() ) );
	}

	private CompletionStage<Void> populateDb() {
		return getSessionFactory().withTransaction( s -> s.persist(  getPerson() ) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		Person johnny = new Person( 999, "Johnny English", Arrays.asList( "888", "555" ) );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( johnny )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, johnny.getId() ) )
				.invoke( found -> assertPhones( context, found, "888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) ) )
		);
	}


	@Test
	public void addOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add( "000" ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( "000" ) )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(VertxTestContext context){
		test ( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( person -> assertPhones( context, person, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( new HashSet<>( List.of( "555" ) ) );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertNotNull( foundPerson );
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( new HashSet<>( List.of( "555" ) ) );
						} ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove ) ) )
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
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.call( session::remove )
						.call( session::flush ) ) )
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

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333" ) )
				// Check initial person collection hasn't changed
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333" ) )
				// Check initial person collection hasn't changed
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson ) )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						.invoke( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson ) )
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

	/**
	 * Utility method to check the content of the collection of elements.
	 * It sorts the expected and actual phones before comparing.
	 */
	private static void assertPhones(VertxTestContext context, Person person, String... expectedPhones) {
		assertNotNull( person );
		String[] sortedExpected = Arrays.stream( expectedPhones ).sorted()
				.sorted( String.CASE_INSENSITIVE_ORDER )
				.collect( Collectors.toList() )
				.toArray( new String[expectedPhones.length] );
		List<String> sortedActual = person.getPhones().stream()
				.sorted( String.CASE_INSENSITIVE_ORDER )
				.collect( Collectors.toList() );
		org.assertj.core.api.Assertions.assertThat( sortedActual )
				.containsExactly( sortedExpected );
	}

	@Entity(name = "Person")
	@Table(name = "Person")
	static class Person {
		@Id
		private Integer id;
		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		private Set<String> phones;

		public Person() {
		}

		public Person(Integer id, String name, Collection<String> phones) {
			this.id = id;
			this.name = name;
			this.phones = new HashSet<>( phones );
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

		public void setPhones(Set<String> phones) {
			this.phones = phones;
		}

		public Set<String> getPhones() {
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
			final StringBuilder sb = new StringBuilder();
			sb.append( id );
			sb.append( ", " ).append( name );
			sb.append( ", ").append( phones );
			return sb.toString();
		}
	}
}

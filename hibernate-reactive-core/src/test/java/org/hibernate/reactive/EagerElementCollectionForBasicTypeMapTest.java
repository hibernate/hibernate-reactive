/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests @{@link ElementCollection} on a {@link Map} of basic types.
 * <p>
 * Example:
 * {@code
 * class Person {
 *
 * @ElementCollection Map<String, String> phones;
 * }
 * }
 * </p>,
 */
public class EagerElementCollectionForBasicTypeMapTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		thePerson = new Person( 7242000, "Claude" );
		thePerson.getPhones().put( "aaaa", "999-999-9999" );
		thePerson.getPhones().put( "bbbb", "111-111-1111" );
		thePerson.getPhones().put( "cccc", "123-456-7890" );

		test( context, openMutinySession().chain( session -> session
				.persist( thePerson ).call( session::flush ) ) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		Map<String, String> phones = new HashMap<>();
		phones.put( "aaaa", "888" );
		phones.put( "bbbb", "555" );
		Person johnny = new Person( 999, "Johnny English", phones );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( johnny )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, johnny.getId() ) )
				.invoke( found -> assertPhones( context, found, "888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().put( "dddd", "000" ) )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( "bbbb" ) )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(VertxTestContext context){
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(VertxTestContext context){
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "bbbb" );
							foundPerson.getPhones().put( "dddd", "000" );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(VertxTestContext context){
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( new HashMap<>() );
							foundPerson.getPhones().put( "aaaa", "555" );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( Assertions::assertNull )
				// Check with native query that the table is empty
				.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
				.thenAccept( resultList -> assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty" );
		secondPerson.getPhones().put( "aaaa", "222-222-2222" );
		secondPerson.getPhones().put( "bbbb", "333-333-3333" );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) )
				// Check new person collection
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
	public void persistCollectionOfNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty" );
		secondPerson.getPhones().put( "xxx", null );
		secondPerson.getPhones().put( "yyy", null );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty" );
		secondPerson.getPhones().put( "xxx", null );
		secondPerson.getPhones().put( "ggg", "567" );
		secondPerson.getPhones().put( "yyy", null );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson ) )
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

	private static void assertPhones(VertxTestContext context, Person person, String... phones) {
		assertNotNull( person );
		assertEquals( phones.length, person.getPhones().size() );
		for ( String number : phones ) {
			assertTrue( phonesContainNumber( person, number ) );
		}
	}

	private static boolean phonesContainNumber(Person person, String phoneNum) {
		for ( String phone : person.getPhones().values() ) {
			if ( phone.equals( phoneNum ) ) {
				return true;
			}
		}
		return false;
	}

	@Entity(name = "Person")
	@Table(name = "Person")
	static class Person {
		@Id
		private Integer id;
		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		private Map<String, String> phones = new HashMap<>();

		public Person() {
		}

		public Person(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Person(Integer id, String name, Map<String, String> phones) {
			this( id, name );
			this.phones = new HashMap<>( phones );
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

		public void setPhones(Map<String, String> phones) {
			this.phones = phones;
		}

		public Map<String, String> getPhones() {
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
			sb.append( ", " ).append( phones );
			return sb.toString();
		}
	}
}

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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout( value = 5, timeUnit = TimeUnit.MINUTES )
public class EagerElementCollectionForEmbeddedEmbeddableMapTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	private Person getPerson() {
		if( thePerson == null ) {
			thePerson = new Person( 7242000, "Claude", new Phone( MOBILE, "000") );
			thePerson.addAlternatePhone( "aaaa", HOME, "111" );
			thePerson.addAlternatePhone( "bbbb", WORK, "222" );
			thePerson.addAlternatePhone( "cccc", HOME, "333" );
		}
		return thePerson;
	}

	private CompletionStage<Void> populateDb() {
		return getSessionFactory().withTransaction( s -> s.persist(  getPerson() ) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		Person johnny = new Person( 999, "Johnny English", new Phone( MOBILE, "999") );
		johnny.addAlternatePhone( "aaaa", HOME, "888" );
		johnny.addAlternatePhone( "bbbb", WORK, "777" );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( johnny )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( s -> s.find( Person.class, johnny.getId() ) )
				.invoke( found -> assertPhones( context, found, "999", "888", "777" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( found -> assertPhones( context, found, "000", "111", "222", "333" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.thenAccept( foundPerson -> foundPerson.addAlternatePhone( "dddd", WORK, "444" ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "000","111", "222", "333", "444" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.removeAlternativePhone( "cccc" ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertFalse( foundPerson.getAlternativePhones().isEmpty() );
							foundPerson.getAlternativePhones().clear();
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertTrue( changedPerson.getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.removeAlternativePhone( "cccc" );
							foundPerson.addAlternatePhone( "dddd", MOBILE, "444" );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "111", "222", "444" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							assertFalse( foundPerson.getAlternativePhones().isEmpty() );
							foundPerson.getPhone().setAlternativePhones( new HashMap<>() );
							foundPerson.addAlternatePhone( "aaaa", WORK, "555" );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) ) )
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
		Person secondPerson = new Person( 9910000, "Kitty", new Phone( MOBILE, "123") );
		secondPerson.addAlternatePhone( "aaaa", HOME, "666" );
		secondPerson.addAlternatePhone( "bbbb", WORK, "777" );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "123", "666", "777" ) )
				// Check initial person collection hasn't changed
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222", "333" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", new Phone( MOBILE, "123") );
		secondPerson.getAlternativePhones().put( "xxx", null );
		secondPerson.getAlternativePhones().put( "yyy", null );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertTrue( foundPerson.getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", new Phone( MOBILE, "123") );
		secondPerson.getAlternativePhones().put( "xxx", null );
		secondPerson.getAlternativePhones().put("ggg", new AlternativePhone(MOBILE, "567" ));
		secondPerson.getAlternativePhones().put( "yyy", null );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "123", "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							assertFalse( found.getAlternativePhones().isEmpty() );
							found.getPhone().setAlternativePhones( null );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000" ) )
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

	private static void assertPhones(VertxTestContext context, Person person, String mainPhone, String... phones) {
		assertNotNull( person );
		assertEquals( mainPhone, person.getPhone().number );
		assertEquals( phones.length, person.getAlternativePhones().size() );
		for ( String number : phones) {
			assertTrue( phonesContainNumber( person, number) );
		}
	}

	private static boolean phonesContainNumber(Person person, String phoneNum) {
		for ( AlternativePhone phone : person.getAlternativePhones().values() ) {
			if ( phone.getNumber().equals( phoneNum ) ) {
				return true;
			}
		}
		return false;
	}

	private static final String MOBILE = "mobile";
	private static final String HOME = "home";
	private static final String WORK = "work";

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

		private Phone phone;

		public Person() {
		}

		public Person(Integer id, String name, Phone phone) {
			this.id = id;
			this.name = name;
			this.phone = phone;
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

		public Phone getPhone() {
			return phone;
		}

		public Map<String, AlternativePhone> getAlternativePhones() {
			return phone.getAlternativePhones();
		}

		public void addAlternatePhone(String id, String type, String number) {
			phone.addAlternatePhone( id, type, number );
		}

		public void removeAlternativePhone(String id) {
			getAlternativePhones().remove(id);
		}
	}

	@Embeddable
	public static class Phone {
		@Column(name = "`type`")
		private String type;

		@Column(name = "`number`")
		private String number;

		@ElementCollection(fetch = FetchType.EAGER)
		private Map<String, AlternativePhone> phones = new HashMap<>();

		public Phone() {
		}

		public Phone(String type, String number) {
			this.type = type;
			this.number = number;
		}

		public String getType() {
			return type;
		}

		public String getNumber() {
			return number;
		}

		public void addAlternatePhone(String id, String type, String number) {
			if( phones == null ) {
				phones = new HashMap<>();
			}
			phones.put( id, new AlternativePhone( type, number));
		}

		public void setAlternativePhones(Map<String, AlternativePhone> phones) {
			this.phones  = phones;
		}

		public Map<String, AlternativePhone> getAlternativePhones() {
			return phones;
		}

	}

	@Embeddable
	public static class AlternativePhone {
		@Column(name = "`type`")
		private String type;

		@Column(name = "`number`")
		private String number;

		public AlternativePhone() {
		}

		public AlternativePhone(String type, String number) {
			this.type = type;
			this.number = number;
		}

		public String getType() {
			return type;
		}

		public String getNumber() {
			return number;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			AlternativePhone phone = (AlternativePhone) o;
			return Objects.equals( number, phone.number );
		}

		@Override
		public int hashCode() {
			return Objects.hash( number );
		}

	}
}

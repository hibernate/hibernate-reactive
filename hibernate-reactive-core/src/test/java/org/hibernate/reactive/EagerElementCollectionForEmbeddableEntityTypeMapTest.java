/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class EagerElementCollectionForEmbeddableEntityTypeMapTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		thePerson = new Person( 7242000, "Claude" );
		thePerson.getPhones().put( "aaaa", new Phone("999-999-9999" ) );
		thePerson.getPhones().put( "bbbb", new Phone("111-111-1111" ) );
		thePerson.getPhones().put( "cccc", new Phone("123-456-7890" ) );

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Person" ) );
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Map<String, Phone> phones = new HashMap<>();
		phones.put( "aaaa", new Phone( "888" ) );
		phones.put( "bbbb", new Phone( "555" ) );
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
	public void findEntityWithElementCollectionWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().put( "dddd", new Phone( "000" ) ) )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(TestContext context) {
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
	public void clearCollectionOfElementsWithStageAPI(TestContext context){
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(TestContext context){
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "bbbb" );
							foundPerson.getPhones().put( "dddd", new Phone( "000" ) );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(TestContext context){
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( new HashMap<>() );
							foundPerson.getPhones().put( "aaaa", new Phone( "555" ) );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( context::assertNull )
				// Check with native query that the table is empty
				.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
				.thenAccept( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty" );
		secondPerson.getPhones().put( "aaaa", new Phone( "222-222-2222" ) );
		secondPerson.getPhones().put( "bbbb", new Phone( "333-333-3333" ) );

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
	public void persistCollectionOfNullsWithStageAPI(TestContext context) {
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
				.thenAccept( foundPerson -> context.assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty" );
		secondPerson.getPhones().put( "xxx", null );
		secondPerson.getPhones().put( "ggg", new Phone( "567" ) );
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
	public void setCollectionToNullWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							context.assertFalse( found.getPhones().isEmpty() );
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

	private static void assertPhones(TestContext context, Person person, String... phones) {
		context.assertNotNull( person );
		context.assertEquals( phones.length, person.getPhones().size() );
		for ( String number : phones) {
			context.assertTrue( phonesContainNumber( person, number) );
		}
	}

	private static boolean phonesContainNumber(Person person, String phoneNum) {
		for ( Phone phone : person.getPhones().values() ) {
			if ( phone.getNumber().equals( phoneNum ) ) {
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
		private Map<String, Phone> phones = new HashMap<>();

		public Person() {
		}


		public Person(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Person(Integer id, String name, Map<String, Phone> phones) {
			this(id, name);
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

		public void setPhones(Map<String, Phone> phones) {
			this.phones = phones;
		}

		public Map<String, Phone> getPhones() {
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

	@Embeddable
	public static class Phone {

		@Column(name = "`number`")
		private String number;

		private String country;

		public Phone() {
		}

		public Phone(String number) {
			this( "UK", number );
		}

		public Phone(String country, String number) {
			this.country = country;
			this.number = number;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getNumber() {
			return number;
		}

		public void setNumber(String number) {
			this.number = number;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Phone phone = (Phone) o;
			return Objects.equals( number, phone.number );
		}

		@Override
		public int hashCode() {
			return Objects.hash( number );
		}
	}
}

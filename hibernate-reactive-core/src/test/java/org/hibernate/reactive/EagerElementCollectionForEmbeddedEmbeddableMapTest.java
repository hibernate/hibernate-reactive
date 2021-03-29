/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
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

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class EagerElementCollectionForEmbeddedEmbeddableMapTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {

		thePerson = new Person( 7242000, "Claude", new Phone( MOBILE, "000") );
		thePerson.addAlternatePhone( "aaaa", HOME, "111" );
		thePerson.addAlternatePhone( "bbbb", WORK, "222" );
		thePerson.addAlternatePhone( "cccc", HOME, "333" );

		Mutiny.Session session = openMutinySession();
		test( context, session.persist( thePerson ).call( session::flush ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Person" ) );
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Person johnny = new Person( 999, "Johnny English", new Phone( MOBILE, "999") );
		johnny.addAlternatePhone( "aaaa", HOME, "888" );
		johnny.addAlternatePhone( "bbbb", WORK, "777" );

		test (
				context,
				session.persist( johnny )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, johnny.getId() ) )
						.invoke( found -> assertPhones( context, found, "999", "888", "777" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId() )
						.thenAccept( found -> assertPhones( context, found, "000", "111", "222", "333" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.thenAccept( foundPerson -> foundPerson.addAlternatePhone( "dddd", WORK,  "444" ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson ->
											 assertPhones(
													 context,
													 updatedPerson,
													 "000","111", "222", "333", "444"
											 ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.removeAlternativePhone( "cccc" ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertFalse( foundPerson.getAlternativePhones().isEmpty() );
							foundPerson.getAlternativePhones().clear();
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() )
								.thenAccept(
										changedPerson ->
												context.assertTrue( changedPerson.getAlternativePhones().isEmpty() ) )
						)
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.removeAlternativePhone( "cccc" );
							foundPerson.addAlternatePhone( "dddd", MOBILE, "444"  );
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "111", "222", "444" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							context.assertFalse( foundPerson.getAlternativePhones().isEmpty() );
							foundPerson.getPhone().setAlternativePhones( new HashMap<>()  );
							foundPerson.addAlternatePhone( "aaaa", WORK, "555" );
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId()) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( foundPerson -> session.remove( foundPerson ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( nullPerson -> context.assertNull( nullPerson ) )
						// Check with native query that the table is empty
						.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
						.thenAccept( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", new Phone( MOBILE, "123") );
		secondPerson.addAlternatePhone( "aaaa", HOME, "666" );
		secondPerson.addAlternatePhone( "bbbb", WORK, "777" );

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "123", "666", "777" ) )
					  // Check initial person collection hasn't changed
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222", "333" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", new Phone( MOBILE, "123") );
		secondPerson.getAlternativePhones().put( "xxx", null );
		secondPerson.getAlternativePhones().put( "xxx", null );

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .thenAccept( foundPerson -> context.assertTrue( foundPerson.getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", new Phone( MOBILE, "123") );
		secondPerson.getAlternativePhones().put( "xxx", null );
		secondPerson.getAlternativePhones().put("ggg", new AlternativePhone(MOBILE, "567" ));
		secondPerson.getAlternativePhones().put( "yyy", null );

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "123", "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test( context,
			  session.find( Person.class, thePerson.getId() )
					  .thenAccept( found -> {
						  context.assertFalse( found.getAlternativePhones().isEmpty() );
						  found.getPhone().setAlternativePhones( null );
					  } )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "000" ) )
		);
	}

	/**
	 * With the Stage API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person.
	 */
	private CompletionStage<List<Object>> selectFromPhonesWithStage(Person person) {
		return openSession()
				.createNativeQuery( "SELECT * FROM Person_phones where Person_id = ?" )
				.setParameter( 1, person.getId() )
				.getResultList();
	}

	private static void assertPhones(TestContext context, Person person, String mainPhone, String... phones) {
		context.assertNotNull( person );
		context.assertEquals( mainPhone, person.getPhone().number );
		context.assertEquals( phones.length, person.getAlternativePhones().size() );
		for ( String number : phones) {
			context.assertTrue( phonesContainNumber( person, number) );
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

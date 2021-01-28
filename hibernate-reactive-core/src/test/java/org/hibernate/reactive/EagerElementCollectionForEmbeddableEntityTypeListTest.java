/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.Before;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

/**
 * Tests @{@link ElementCollection} on a {@link java.util.Set} of basic types.
 * <p>
 * Example:
 * {@code
 *     class Person {
 *         @ElementCollection
 *         List<Phone> phones;
 *     }
 *
 *
 *     @Embeddable
 *     public static class Phone {
 *     ...
 *     }
 * }
 * </p>,
 *
 * @see EagerElementCollectionForBasicTypeListTest
 * @see EagerElementCollectionForBasicTypeSetTest
 */
public class EagerElementCollectionForEmbeddableEntityTypeListTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "999-999-9999" ) );
		phones.add( new Phone( "111-111-1111" ) );
		thePerson = new Person( 777777, "Claude", phones );

		Stage.Session session = openSession();

		test( context, session.persist( thePerson )
				.thenCompose( v -> session.flush() )
		);
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "888" ) );
		phones.add( new Phone( "555" ) );
		Person johnny = new Person( 999, "Johnny English", phones );

		test (
				context,
				session.persist( johnny )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, johnny.getId() ) )
						.invoke( found -> assertPhones( context, found, "888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test ( context, session
				.find( Person.class, thePerson.getId() )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson,"999-999-9999", "111-111-1111" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test ( context, session
				.find( Person.class, thePerson.getId() )
				.invoke( foundPerson -> assertPhones( context, foundPerson,"999-999-9999", "111-111-1111" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add(new Phone("000" )) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson ->
											 assertPhones(
													 context,
													 updatedPerson,
													 "999-999-9999", "111-111-1111", "000"
											 ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		Person thomas = new Person( 7, "Thomas Reaper", phones );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thomas.getId() ) )
						.thenAccept( found -> assertPhones( context, found, "111", "111", "111", "111") )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		Person thomas = new Person( 567, "Thomas Reaper", phones );

		test(
				context,
				session.persist( thomas )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thomas.getId() ) )
						.invoke( found -> assertPhones( context, found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> {
							Stage.Session newSession = openSession();
							return newSession.find( Person.class, thomas.getId() )
									// Change one of the element in the collection
									.thenAccept( found -> {
										found.getPhones().set( 1, new Phone( "47" ) );
										found.getPhones().set( 3,  new Phone( "47" ) );
									} )
									.thenCompose( ignore -> newSession.flush() )
									.thenCompose( ignore -> openSession().find( Person.class, thomas.getId() ) )
									.thenAccept( found -> assertPhones( context, found, "000", "47", "000", "47" ) );
						} )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test(
				context,
				session.persist( thomas )
						.call( session::flush )
						.chain( () -> {
							Mutiny.Session newSession = openMutinySession();
							return newSession.find( Person.class, thomas.getId() )
									// Change a couple of the elements in the collection
									.invoke( found -> {
										found.getPhones().set( 1, new Phone( "47" ) );
										found.getPhones().set( 3,  new Phone( "47" ) );
									} )
									.call( newSession::flush )
									.chain( () -> openMutinySession().find( Person.class, thomas.getId() ) )
									.invoke( found -> assertPhones( context, found, "000", "47", "000", "47" ) );
						} )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> {
							Stage.Session newSession = openSession();
							return newSession.find( Person.class, thomas.getId() )
									// Change one of the element in the collection
									.thenAccept( found -> {
										// it doesn't matter which elements are deleted because they are all equal
										found.getPhones().remove( 1 );
										found.getPhones().remove( 2 );
									} )
									.thenCompose( ignore -> newSession.flush() )
									.thenCompose( ignore -> openSession().find( Person.class, thomas.getId() ) )
									.thenAccept( found -> assertPhones( context, found, "000", "000" ) );
						} )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test(
				context,
				session.persist( thomas )
						.call( session::flush )
						.chain( () -> {
							Mutiny.Session newSession = openMutinySession();
							return newSession.find( Person.class, thomas.getId() )
									// Change one of the element in the collection
									.invoke( found -> {
										// it doesn't matter which elements are deleted because they are all equal
										found.getPhones().remove( 1 );
										found.getPhones().remove( 2 );
									} )
									.call( newSession::flush )
									.chain( () -> openMutinySession().find( Person.class, thomas.getId() ) )
									.invoke( found -> assertPhones( context, found, "000", "000" ) );
						} )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( new Phone("000" ) ) )
						.call( session::flush )
						// Check new person collection
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( updatedPerson ->
										 assertPhones(
												 context,
												 updatedPerson,
												 "999-999-9999", "111-111-1111", "000"
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
						.thenAccept( foundPerson -> { foundPerson.getPhones().remove( new Phone( "999-999-9999" ) ); } )
						.thenCompose( v -> session.flush())
						.thenCompose( v -> openSession()
								.find( Person.class, thePerson.getId() )
								.thenAccept( foundPerson -> assertPhones( context, foundPerson, "111-111-1111" ) )
						)
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> { foundPerson.getPhones().remove( new Phone( "999-999-9999" ) ); } )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() )
								.invoke( foundPerson -> assertPhones( context, foundPerson, "111-111-1111" ) ) )
		);
	}

	@Test
	public void clearCollectionElementsStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// clear collection
						.thenAccept( foundPerson -> foundPerson.getPhones().clear() )
						.thenCompose( v -> session.flush() )
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() )
								.thenAccept( changedPerson -> assertPhones( context, changedPerson))) //context.assertTrue( changedPerson.getPhones().isEmpty() ) ) )
		);
	}

	@Test
	public void clearCollectionElementsMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// clear collection
						.invoke( foundPerson -> foundPerson.getPhones().clear() )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() )
								.invoke( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) ) )
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
							foundPerson.getPhones().remove( new Phone("111-111-1111") );
							foundPerson.getPhones().add( new Phone("000") );
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "000" ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(TestContext context){
		Mutiny.Session session = openMutinySession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.invoke( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhones().remove(  new Phone("111-111-1111") );
							foundPerson.getPhones().add( new Phone("000") );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( person -> assertPhones( context, person, "999-999-9999", "000" ) )
		);
	}

	@Test
	public void replaceSecondCollectionElementStageAPI(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							// remove existing phone and add new phone
							foundPerson.getPhones().remove( new Phone( "999-999-9999" ) );
							foundPerson.getPhones().add( new Phone( "000-000-0000" ) );
						} )
						.thenCompose(v -> session.flush())
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000-000-0000", "111-111-1111" ) )
		);
	}

	@Test
	public void replaceSecondCollectionElementMutinyAPI(TestContext context){
		Mutiny.Session session = openMutinySession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.invoke( foundPerson -> {
							// remove existing phone and add new phone
							foundPerson.getPhones().remove( new Phone( "999-999-9999" ) );
							foundPerson.getPhones().add( new Phone( "000-000-0000" ) );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> assertPhones( context, changedPerson, "000-000-0000", "111-111-1111" ) )
		);
	}

	@Test
	public void setNewElementCollectionStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						// replace phones with list of 1 phone
						.thenAccept( foundPerson -> {
							foundPerson.setPhones( Arrays.asList( new Phone( "000-000-0000" ) ) );
						} )
						.thenCompose(v -> session.flush())
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000-000-0000" ) )
		);
	}

	@Test
	public void setNewElementCollectionMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						// replace phones with list of 1 phone
						.invoke( foundPerson -> {
							foundPerson.setPhones( Arrays.asList( new Phone( "000-000-0000" ) ) );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> assertPhones( context, changedPerson, "000-000-0000" ) )
		);
	}

	@Test
	public void removePersonStageAPI(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( foundPerson -> session.remove( foundPerson ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId()) )
						.thenAccept( nullPerson ->  context.assertNull( nullPerson ) )
						// Check with native query that the table is empty
						.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
						.thenAccept( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void removePersonMutinyAPI(TestContext context){
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.call( session::remove )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId()) )
						.invoke( nullPerson ->  context.assertNull( nullPerson ) )
						// Check with native query that the table is empty
						.chain( () -> selectFromPhonesWithMutiny( thePerson ) )
						.invoke( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty",
										  Arrays.asList(
												  new Phone( "222-222-2222" ),
												  new Phone( "333-333-3333" ),
												  new Phone( "444-444-4444" )
										  )
		);

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333", "444-444-4444" ) )
					  // Check initial person collection hasn't changed
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty",
										  Arrays.asList(
												  new Phone( "222-222-2222" ),
												  new Phone( "333-333-3333" ),
												  new Phone( "444-444-4444" )
										  )
		);

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333", "444-444-4444" ) )
					  // Check initial person collection hasn't changed
					  .chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .thenAccept( foundPerson -> context.assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .invoke( foundPerson -> context.assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, new Phone( "567" ), null ) );

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, new Phone( "567" ), null ) );

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test( context,
			  session.find( Person.class, thePerson.getId() )
					  .thenAccept( found -> {
						  context.assertFalse( found.getPhones().isEmpty() );
						  found.setPhones( null );
					  } )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson ) )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test( context,
			  session.find( Person.class, thePerson.getId() )
					  .invoke( found -> {
						  context.assertFalse( found.getPhones().isEmpty() );
						  found.setPhones( null );
					  } )
					  .call( session::flush )
					  .chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson ) )
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

	/**
	 * With the Mutiny API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person
	 */
	private Uni<List<Object>> selectFromPhonesWithMutiny(Person person) {
		return openMutinySession()
				.createNativeQuery( "SELECT * FROM Person_phones where Person_id = ?" )
				.setParameter( 1, person.getId() )
				.getResultList();
	}

	private static void assertPhones(TestContext context, Person person, String... phones) {
		context.assertNotNull( person );
		context.assertEquals( phones.length, person.getPhones().size() );
		for ( String number : phones) {
			context.assertTrue( phonesContainNumber( person, number) );
		}
	}

	private static boolean phonesContainNumber(Person person, String phoneNum) {
		for ( Phone phone : person.getPhones() ) {
			if ( phone.getNumber().equals( phoneNum ) ) {
				return true;
			}
		}
		return false;
	}

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		private List<Phone> phones = new ArrayList<>();

		public Person() {
		}

		public Person(Integer id, String name, List<Phone> phones) {
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

		public Phone getPhone(String number) {
			for( Phone phone : getPhones() ) {
				if( phone.getNumber().equals( number ) ) {
					return phone;
				}
			}
			return null;
		}

		public void setPhones(List<Phone> phones) {
			this.phones  = phones;
		}

		public List<Phone> getPhones() {
			return phones;
		}
	}

	@Embeddable
	public static class Phone {

		private String number;

		private String country;

		// FIXME: add nested @Embeddable
		// @ElementCollection(fetch = FetchType.EAGER)
		// private List<AlternativePhone> alternatives = new ArrayList<>();

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

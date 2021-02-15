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
import javax.persistence.Column;
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
 * @Embeddable
 * public static class Phone {
 *    @ElementCollection
 *    private List<AlternativePhone> alternatives = new ArrayList<>();
 * }
 *
 * @Embeddable
 * public static class AlternativePhone {
 *    ...
 * }
 * </p>,
 *
 * @see EagerElementCollectionForEmbeddableEntityTypeListTest
 */
public class EagerElementCollectionForEmbeddedEmbeddableTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		Phone phone = new Phone(MOBILE, "000");
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( WORK, "222" );

		thePerson = new Person( 777777, "Claude", phone );

		Stage.Session session = openSession();

		test( context, session.persist( thePerson )
				.thenCompose( v -> session.flush() )
		);
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Phone phone = new Phone(MOBILE, "444");
		phone.addAlternatePhone( HOME, "888" );
		phone.addAlternatePhone( WORK, "555" );

		Person johnny = new Person( 999, "Johnny English", phone );

		test (
				context,
				session.persist( johnny )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, johnny.getId() ) )
						.invoke( found -> assertPhones( context, found, "444","888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test ( context, session
				.find( Person.class, thePerson.getId() )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson,"000", "111", "222" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test ( context, session
				.find( Person.class, thePerson.getId() )
				.invoke( foundPerson -> assertPhones( context, foundPerson,"000", "111", "222" ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhone().getAlternativePhones().add(new AlternativePhone( MOBILE, "333" )) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson ->
											 assertPhones(
													 context,
													 updatedPerson,
													 "000", "111", "222", "333"
											 ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.invoke( foundPerson -> foundPerson.getPhone().getAlternativePhones().add(new AlternativePhone( MOBILE, "333" )) )
						.call( session::flush )
						// Check new person collection
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( updatedPerson ->
										 assertPhones(
												 context,
												 updatedPerson,
												 "000", "111", "222", "333"
										 ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );

		Person thomas = new Person( 7, "Thomas Reaper", phone );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thomas.getId() ) )
						.thenAccept( found -> assertPhones( context, found, "555", "111", "111", "111", "111") )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );

		Person thomas = new Person( 567, "Thomas Reaper", phone );

		test(
				context,
				session.persist( thomas )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thomas.getId() ) )
						.invoke( found -> assertPhones( context, found, "555", "111", "111", "111", "111") )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> {
							Stage.Session newSession = openSession();
							return newSession.find( Person.class, thomas.getId() )
									// Change one of the element in the collection
									.thenAccept( found -> {
										found.getPhone().getAlternativePhones().set( 1, new AlternativePhone( MOBILE, "47" ) );
										found.getPhone().getAlternativePhones().set( 3,  new AlternativePhone( MOBILE, "47" ) );
									} )
									.thenCompose( ignore -> newSession.flush() )
									.thenCompose( ignore -> openSession().find( Person.class, thomas.getId() ) )
									.thenAccept( found -> assertPhones( context, found, "555", "000", "47", "000", "47" ) );
						} )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

		test(
				context,
				session.persist( thomas )
						.call( session::flush )
						.chain( () -> {
							Mutiny.Session newSession = openMutinySession();
							return newSession.find( Person.class, thomas.getId() )
									// Change a couple of the elements in the collection
									.invoke( found -> {
										found.getPhone().getAlternativePhones().set( 1, new AlternativePhone( MOBILE, "47" ) );
										found.getPhone().getAlternativePhones().set( 3,  new AlternativePhone( MOBILE, "47" ) );
									} )
									.call( newSession::flush )
									.chain( () -> openMutinySession().find( Person.class, thomas.getId() ) )
									.invoke( found -> assertPhones( context, found, "555", "000", "47", "000", "47" ) );
						} )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

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
										found.getPhone().getAlternativePhones().remove( 1 );
										found.getPhone().getAlternativePhones().remove( 2 );
									} )
									.thenCompose( ignore -> newSession.flush() )
									.thenCompose( ignore -> openSession().find( Person.class, thomas.getId() ) )
									.thenAccept( found -> assertPhones( context, found, "555", "000", "000" ) );
						} )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

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
										found.getPhone().getAlternativePhones().remove( 1 );
										found.getPhone().getAlternativePhones().remove( 2 );
									} )
									.call( newSession::flush )
									.chain( () -> openMutinySession().find( Person.class, thomas.getId() ) )
									.invoke( found -> assertPhones( context, found, "555", "000", "000" ) );
						} )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson ->
											 foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE, "222" ) ) )
						.thenCompose( v -> session.flush())
						.thenCompose( v -> openSession()
								.find( Person.class, thePerson.getId() )
								.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111" ) ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE, "222" ) ) )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() )
								.invoke( foundPerson -> assertPhones( context, foundPerson, "000", "111" ) ) )
		);
	}

	@Test
	public void clearCollectionElementsStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// clear collection
						.thenAccept( foundPerson -> foundPerson.getPhone().getAlternativePhones().clear() )
						.thenCompose( v -> session.flush() )
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() )
								.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000")))
		);
	}

	@Test
	public void clearCollectionElementsMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// clear collection
						.invoke( foundPerson -> foundPerson.getPhone().getAlternativePhones().clear() )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() )
								.invoke( changedPerson -> assertPhones( context, changedPerson, "000")))
		);
	}

	public void removeAndAddElementWithStageAPI(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE, "111") );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444") );
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "222", "444" ) )
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
							foundPerson.getPhone().getAlternativePhones().remove(  new AlternativePhone( MOBILE, "111") );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444") );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( person -> assertPhones( context, person, "000", "222", "444" ) )
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
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE,  "222" ) );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444" ) );
						} )
						.thenCompose(v -> session.flush())
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "111", "444" ) )
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
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE,  "222" ) );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444" ) );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> assertPhones( context, changedPerson, "000", "111", "444" ) )
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
							foundPerson.getPhone().setAlternativePhones( Arrays.asList( new AlternativePhone( MOBILE, "555" ) ) );
						} )
						.thenCompose(v -> session.flush())
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "555" ) )
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
							foundPerson.getPhone().setAlternativePhones( Arrays.asList( new AlternativePhone( MOBILE, "555" ) ) );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> assertPhones( context, changedPerson, "000", "555" ) )
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
		Phone phone = new Phone( MOBILE, "999");
		Person secondPerson = new Person( 9910000, "Kitty", phone);
		phone.setAlternativePhones(
				Arrays.asList(
					  new AlternativePhone( HOME, "222" ),
					  new AlternativePhone( WORK, "333" ),
					  new AlternativePhone( MOBILE, "444" )
			  )
		);

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "999", "222", "333", "444" ) )
					  // Check initial person collection hasn't changed
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(TestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		Person secondPerson = new Person( 9910000, "Kitty", phone);
		phone.setAlternativePhones(
				Arrays.asList(
						new AlternativePhone( HOME, "222" ),
						new AlternativePhone( WORK, "333" ),
						new AlternativePhone( MOBILE, "444" )
				)
		);

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "999", "222", "333", "444" ) )
					  // Check initial person collection hasn't changed
					  .chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(TestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .thenAccept( foundPerson -> context.assertTrue( foundPerson.getPhone().getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(TestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .invoke( foundPerson -> context.assertTrue( foundPerson.getPhone().getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(TestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, new AlternativePhone( HOME, "222" ), null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "999", "222" ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(TestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, new AlternativePhone( HOME, "222" ), null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  // Null values don't get persisted
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "999", "222" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test( context,
			  session.find( Person.class, thePerson.getId() )
					  .thenAccept( found -> {
						  context.assertFalse( found.getPhone().getAlternativePhones().isEmpty() );
						  found.getPhone().setAlternativePhones( null );
					  } )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "000" ) )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test( context,
			  session.find( Person.class, thePerson.getId() )
					  .invoke( found -> {
						  context.assertFalse( found.getPhone().getAlternativePhones().isEmpty() );
						  found.getPhone().setAlternativePhones( null );
					  } )
					  .call( session::flush )
					  .chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "000" ) )
		);
	}

	/**
	 * With the Stage API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person.
	 */
	private CompletionStage<List<Object>> selectFromPhonesWithStage(
			Person person) {
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

	private static void assertPhones(TestContext context, Person person, String mainPhone, String... phones) {
		context.assertNotNull( person );
		context.assertNotNull( person.getPhone() != null );
		context.assertEquals( mainPhone, person.getPhone().getNumber() );
		if( phones != null ) {
			context.assertEquals( phones.length, person.getPhone().getAlternativePhones().size() );
			for ( String number : phones ) {
				context.assertTrue( phoneContainsNumber( person.getPhone(), number ) );
			}
		}
	}

	private static boolean phoneContainsNumber(Phone mainPhone, String phoneNum) {
		for ( AlternativePhone phone : mainPhone.getAlternativePhones() ) {
			if ( phone.getNumber().equals( phoneNum ) ) {
				return true;
			}
		}
		return false;
	}

	private static String MOBILE = "mobile";
	private static String HOME = "home";
	private static String WORK = "work";

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

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
		private Phone phone;

		public Phone getPhone() {
			return phone;
		}
	}

	@Embeddable
	public static class Phone {
		@Column(name = "`type`")
		private String type;

		@Column(name = "`number`")
		private String number;

		@ElementCollection(fetch = FetchType.EAGER)
		private List<AlternativePhone> phones = new ArrayList<>();

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

		public void addAlternatePhone(String type, String number) {
			if( phones == null ) {
				phones = new ArrayList<>();
			}
			phones.add(new AlternativePhone(type, number));
		}

		public void setAlternativePhones(List<AlternativePhone> phones) {
			this.phones  = phones;
		}

		public List<AlternativePhone> getAlternativePhones() {
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

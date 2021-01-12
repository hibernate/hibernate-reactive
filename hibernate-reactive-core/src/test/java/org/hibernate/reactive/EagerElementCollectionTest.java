/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

public class EagerElementCollectionTest extends BaseReactiveTest {

	private Person thePerson;

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		String[] phoneArray = new String[]{ "999-999-9999", "111-111-1111", "123-456-7890" };
		thePerson = new Person( 7242000, "Claude", phoneArray);

		Stage.Session session = openSession();

		test( context,
			  session.persist( thePerson )
				.thenCompose( v -> session.flush() )
			    .thenCompose( v -> openSession().find( Person.class, thePerson.getId()) )
		);
	}

	/*
	 * This test performs a simple check of Person entity collection values after persisting
	 */
	@Test
	public void findPhonesElementCollectionStageSession(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenAccept( found -> {
							context.assertNotNull( found );
							context.assertEquals( 3, found.getPhones().size() );
							context.assertTrue( found.getPhones().contains( "999-999-9999" ) );
							context.assertTrue( found.getPhones().contains( "123-456-7890" ) );
							context.assertTrue( found.getPhones().contains( "111-111-1111" ) );						} )
		);
	}

	/*
	 * remove the first phone element value
	 */
	@Test
	public void removeFirstPhoneElement(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId())
						.thenCompose( foundPerson -> {
							foundPerson.getPhones().remove(0);
							return session.flush();
						} )
						.thenCompose( v -> openSession()
								.find( Person.class, thePerson.getId() )
								.thenAccept( changedPerson -> {
									context.assertEquals( 2, changedPerson.getPhones().size() );
								} )
						)
		);
	}

	/*
	 * clear the phone list and verify the new size() == 0
	 */
	@Test
	public void clearPhoneElements(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						.thenCompose( foundPerson -> {
							foundPerson.getPhones().clear();
							return session.flush();
						} )
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId() )
						.thenAccept( changedPerson -> {
							context.assertEquals( 0, changedPerson.getPhones().size() );
						} )
				)
		);
	}

	/*
	 * perform a set(index) with a new phone number on the phones list and check expected value
	 */
	@Test
	public void replaceSecondPhoneElement(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenCompose( foundPerson -> {
										 context.assertNotNull( foundPerson );
										 context.assertEquals( 3, foundPerson.getPhones().size() );
										 foundPerson.getPhones().set( 1, "000-000-0000" );
										 return session.flush();
									 }
						)
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId()) )
						.thenAccept( changedPerson -> {
							context.assertNotNull( changedPerson );
							context.assertEquals( 3, changedPerson.getPhones().size() );
							// Can't assume order so need to check all values in the list
							context.assertTrue( changedPerson.getPhones().contains( "999-999-9999" ) );
							context.assertTrue( changedPerson.getPhones().contains( "123-456-7890" ) );
							context.assertTrue( changedPerson.getPhones().contains( "000-000-0000" ) );
						} )
		);
	}

	/*
	 * Overwrite the phone list with a new String list with 1 value
	 */
	@Test
	public void setNewElementCollection(TestContext context){
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenCompose( foundPerson -> {
										 context.assertNotNull( foundPerson );
										 context.assertEquals( 3, foundPerson.getPhones().size() );
										 List<String> nums = new ArrayList();
										 nums.add("000 000 0000");
										 foundPerson.setPhones( nums );
										 return session.flush();
									 }
						)
						.thenCompose( s -> openSession().find( Person.class, thePerson.getId()) )
						.thenAccept( changedPerson -> {
							context.assertNotNull( changedPerson );
							context.assertEquals( 1, changedPerson.getPhones().size() );
							context.assertEquals( "000 000 0000",  changedPerson.getPhones().get(0));
						} )
		);
	}

	/*
	 * Removes the person entity and checks if NULL afterwards
	 */
	@Test
	public void removePerson(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId())
						.thenCompose( foundPerson -> session.remove( foundPerson ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId()) )
						.thenAccept( nullPerson ->  context.assertNull( nullPerson ) )
						// add native query to check if Person_phones == NULL
						.thenCompose( v -> openSession().createNativeQuery("SELECT * FROM person_phones where Person_id = " + thePerson.getId()).getResultList())
						.thenAccept( resultList ->  context.assertTrue(  resultList.isEmpty())
						)
		);
	}

	@Test
	public void addAndVerifySecondPerson(TestContext context) {
		// TODO: Maybe it's worthy to have an additional test that creates a different person only used
		//       by the test and then checks with a native query that the right table contains the expected values.
		String[] phoneArray = new String[]{ "222-222-2222", "333-333-3333" };
		Person secondPerson = new Person( 9910000, "Kitty", phoneArray);

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId())
					  .thenAccept( foundPerson -> {
						  context.assertNotNull( foundPerson );
						  context.assertEquals( 2, foundPerson.getPhones().size() );
					  } )
					  .thenCompose( s -> openSession().createNativeQuery("SELECT * FROM person_phones where Person_id = " + secondPerson.getId()).getResultList())
					  .thenAccept( resultList ->  context.assertEquals( 2, resultList.size()) )
					  .thenCompose( s -> openSession().createNativeQuery("SELECT * FROM person_phones where Person_id = " + thePerson.getId()).getResultList())
					  .thenAccept( resultList ->  context.assertEquals( 3, resultList.size()) )
				)

		);
	}

	@Entity(name = "Person")
	public static class Person {
		@Id
		private Integer id;
		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		private List<String> phones = new ArrayList<>();

		/*
		 TODO: We also need to check that all the tests pass with the @OrderColumn annotation
				@ElementCollection(fetch = FetchType.EAGER)
				@OrderColumn(name = "phone_order")
				private List<String> workNumbers = new ArrayList<>();

				I don't know if it's better to keep it in this class or a separate one. Your choice.
		*/

		public Person() {
		}

		public Person(Integer id, String name, String[] phones) {
			this.id = id;
			this.name = name;
			if( phones != null ) {
				this.phones.addAll( Arrays.asList( phones ) );
			}
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
	}

}

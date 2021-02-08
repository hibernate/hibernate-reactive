/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.Before;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;
import org.assertj.core.api.Assertions;

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
 * @see EagerElementCollectionForEmbeddableEntityTypeListTest
 */
public class EagerElementCollectionForBasicTypeListTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		List<String> phones = Arrays.asList( "999-999-9999", "111-111-1111", "123-456-7890" );
		thePerson = new Person( 7242000, "Claude", phones );

		Mutiny.Session session = openMutinySession();
		test( context, session.persist( thePerson ).call( session::flush ) );
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Person johnny = new Person( 999, "Johnny English", Arrays.asList( "888", "555" ) );

		test (
				context,
				session.persist( johnny )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, johnny.getId() ) )
						.invoke( found -> assertPhones( context, found, "888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId() )
						.thenAccept( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test (
				context,
				session.find( Person.class, thePerson.getId() )
						.invoke( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		Person thomas = new Person( 7, "Thomas Reaper", Arrays.asList( "111", "111", "111", "111" ) );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thomas.getId() ) )
						.thenAccept( found -> assertPhones( context, found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		Person thomas = new Person( 567, "Thomas Reaper", Arrays.asList( "111", "111", "111", "111" ) );

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

		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test(
				context,
				session.persist( thomas )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> {
							Stage.Session newSession = openSession();
							return newSession.find( Person.class, thomas.getId() )
									// Change one of the element in the collection
								.thenAccept( found -> {
									found.getPhones().set( 1, "47" );
									found.getPhones().set( 3, "47" );
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

		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test(
				context,
				session.persist( thomas )
						.call( session::flush )
						.chain( () -> {
							Mutiny.Session newSession = openMutinySession();
							return newSession.find( Person.class, thomas.getId() )
									// Change a couple of the elements in the collection
									.invoke( found -> {
										found.getPhones().set( 1, "47" );
										found.getPhones().set( 3, "47" );
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

		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

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

		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

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
	public void addOneElementWithStageAPI(TestContext context) {
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add( "000" ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson ->
											 assertPhones(
													 context,
													 updatedPerson,
													 "999-999-9999", "111-111-1111", "123-456-7890", "000"
											 ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( "000" ) )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( updatedPerson ->
										 assertPhones(
												 context,
												 updatedPerson,
												 "999-999-9999", "111-111-1111", "123-456-7890", "000"
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
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(TestContext context){
		Stage.Session session = openSession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() )
						.thenAccept( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) )
				)
		);
	}

	@Test
	public void clearCollectionOfElementsWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) )
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
										 foundPerson.getPhones().remove( "111-111-1111" );
										 foundPerson.getPhones().add( "000" );
									 } )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "123-456-7890", "000" ) )
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
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( person -> assertPhones( context, person, "999-999-9999", "123-456-7890", "000" ) )
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
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( Arrays.asList( "555" ) );
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> openSession().find( Person.class, thePerson.getId()) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertNotNull( foundPerson );
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( Arrays.asList( "555" ) );
						} )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> assertPhones( context, changedPerson, "555" ) )
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
	public void removePersonWithMutinyAPI(TestContext context) {
		Mutiny.Session session = openMutinySession();

		test(
				context,
				session.find( Person.class, thePerson.getId() )
						.call( session::remove )
						.call( session::flush )
						.chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
						.invoke( nullPerson -> context.assertNull( nullPerson ) )
						// Check with native query that the table is empty
						.chain( () -> selectFromPhonesWithMutiny( thePerson ) )
						.invoke( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		Stage.Session session = openSession();

		test( context,
			  session.persist( secondPerson )
					  .thenCompose( v -> session.flush() )
					  // Check new person collection
					  .thenCompose( v -> openSession().find( Person.class, secondPerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333" ) )
					  // Check initial person collection hasn't changed
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId() ) )
					  .thenAccept( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		Mutiny.Session session = openMutinySession();

		test( context,
			  session.persist( secondPerson )
					  .call( session::flush )
					  // Check new person collection
					  .chain( () -> openMutinySession().find( Person.class, secondPerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333" ) )
					  // Check initial person collection hasn't changed
					  .chain( () -> openMutinySession().find( Person.class, thePerson.getId() ) )
					  .invoke( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) )
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
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

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
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

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

	/**
	 * Utility method to check the content of the collection of elements.
	 * It sorts the expected and actual phones before comparing.
	 */
	private static void assertPhones(TestContext context, Person person, String... expectedPhones) {
		context.assertNotNull( person );
		String[] sortedExpected = Arrays.stream( expectedPhones ).sorted()
				.sorted( String.CASE_INSENSITIVE_ORDER )
				.collect( Collectors.toList() )
				.toArray( new String[expectedPhones.length] );
		List<String> sortedActual = person.getPhones().stream()
				.sorted( String.CASE_INSENSITIVE_ORDER )
				.collect( Collectors.toList() );
		Assertions.assertThat( sortedActual )
				.containsExactly( sortedExpected );
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
			final StringBuilder sb = new StringBuilder();
			sb.append( id );
			sb.append( ", " ).append( name );
			sb.append( ", ").append( phones );
			return sb.toString();
		}
	}
}

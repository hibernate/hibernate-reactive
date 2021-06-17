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
import java.util.stream.Collectors;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;
import org.assertj.core.api.Assertions;

/**
 * Tests @{@link ElementCollection} on a {@link java.util.Set} of basic types.
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
public class EagerElementCollectionForBasicTypeSetTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		Set<String> phones = new HashSet<>( Arrays.asList( "999-999-9999", "111-111-1111", "123-456-7890" ) );
		thePerson = new Person( 7242000, "Claude", phones );

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Person" ) );
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Person johnny = new Person( 999, "Johnny English", Arrays.asList( "888", "555" ) );

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
	public void findEntityWithElementCollectionWithMutinyAPI(TestContext context) {
		test( context, openMutinySession()
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}


	@Test
	public void addOneElementWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add( "000" ) )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(TestContext context) {
		test( context, openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( "000" ) )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithStageAPI(TestContext context){
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void clearCollectionOfElementsWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.getPhones().clear();
						} ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "123-456-7890", "000" ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(TestContext context){
		test ( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( person -> assertPhones( context, person, "999-999-9999", "123-456-7890", "000" ) )
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
							foundPerson.setPhones( new HashSet<>( Arrays.asList( "555" ) ) );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void setNewElementCollectionWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertNotNull( foundPerson );
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( new HashSet<>( Arrays.asList( "555" ) ) );
						} ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "555" ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( context::assertNull )
				// Check with native query that the table is empty
				.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
				.thenAccept( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void removePersonWithMutinyAPI(TestContext context) {
		test( context, openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.call( session::remove )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( context::assertNull )
				// Check with native query that the table is empty
				.chain( () -> selectFromPhonesWithMutiny( thePerson ) )
				.invoke( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) )
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
	public void persistAnotherPersonWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) )
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
	public void persistCollectionOfNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

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
	public void persistCollectionOfNullsWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> context.assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

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
	public void persistCollectionWithNullsWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
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

	@Test
	public void setCollectionToNullWithMutinyAPI(TestContext context) {
		test( context, openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						.invoke( found -> {
							context.assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
						.call( session::flush ) )
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

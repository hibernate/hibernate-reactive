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
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.skipTestsFor;

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
 * @see EagerElementCollectionForEmbeddableTypeListTest
 */
public class EagerElementCollectionForBasicTypeListTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule db2WithVertx4BugRule = skipTestsFor( DB2 );

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

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Person" ) );
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		Person johnny = new Person( 999, "Johnny English", Arrays.asList( "888", "555" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( johnny ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, johnny.getId() ) )
						.invoke( found -> assertPhones( context, found, "888", "555" ) ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithStageAPI(TestContext context) {
		test( context, getSessionFactory().withSession( session -> session
				.find( Person.class, thePerson.getId() )
				.thenAccept( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionWithMutinyAPI(TestContext context) {
		test( context,  getMutinySessionFactory()
				.withSession( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( found -> assertPhones( context, found, "999-999-9999", "111-111-1111", "123-456-7890" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Person thomas = new Person( 7, "Thomas Reaper", Arrays.asList( "111", "111", "111", "111" ) );

		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thomas.getId() ) ) )
				.thenAccept( found -> assertPhones( context, found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Person thomas = new Person( 567, "Thomas Reaper", Arrays.asList( "111", "111", "111", "111" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thomas.getId() ) )
						.invoke( found -> assertPhones( context, found, "111", "111", "111", "111" ) ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.thenCompose( v -> getSessionFactory().withTransaction( (s, t) -> s
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.thenAccept( found -> {
							found.getPhones().set( 1, "47" );
							found.getPhones().set( 3, "47" );
						} ) ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thomas.getId() ) )
						.thenAccept( found -> assertPhones( context, found, "000", "47", "000", "47" ) ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session
								.find( Person.class, thomas.getId() )
								// Change a couple of the elements in the collection
								.invoke( found -> {
									found.getPhones().set( 1, "47" );
									found.getPhones().set( 3, "47" );
								} ) )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thomas.getId() ) )
						.invoke( found -> assertPhones( context, found, "000", "47", "000", "47" ) ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithStageAPI(TestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( Person.class, thomas.getId() )
								// Change one of the element in the collection
								.thenAccept( found -> {
									// it doesn't matter which elements are deleted because they are all equal
									found.getPhones().remove( 1 );
									found.getPhones().remove( 2 );
								} )
						)
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thomas.getId() ) )
						.thenAccept( found -> assertPhones( context, found, "000", "000" ) ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithMutinyAPI(TestContext context) {
		Person thomas = new Person( 47, "Thomas Reaper", Arrays.asList( "000", "000", "000", "000" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persist( thomas ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.find( Person.class, thomas.getId() )
								// Change one of the element in the collection
								.invoke( found -> {
									// it doesn't matter which elements are deleted because they are all equal
									found.getPhones().remove( 1 );
									found.getPhones().remove( 2 );
								} )
						)
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thomas.getId() ) )
						.invoke( found -> assertPhones( context, found, "000", "000" ) ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add( "000" ) ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( "000" ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "111-111-1111", "123-456-7890", "000" ) ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory().withTransaction( (s, tx) -> s
					.find( Person.class, thePerson.getId() )
					// Remove one element from the collection
					.invoke( foundPerson -> foundPerson.getPhones().remove( "111-111-1111" ) )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( updatedPerson -> assertPhones( context, updatedPerson, "999-999-9999", "123-456-7890" ) ) )
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
						} )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) ) )
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
						} )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> context.assertTrue( changedPerson.getPhones().isEmpty() ) ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(TestContext context){
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "999-999-9999", "123-456-7890", "000" ) ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertNotNull( foundPerson );
							foundPerson.getPhones().remove( "111-111-1111" );
							foundPerson.getPhones().add( "000" );
						} )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( person -> assertPhones( context, person, "999-999-9999", "123-456-7890", "000" ) ) )
		);
	}

	@Test
	public void setNewElementCollectionWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							context.assertNotNull( foundPerson );
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( Arrays.asList( "555" ) );
						} )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( changedPerson -> assertPhones( context, changedPerson, "555" ) ) )
		);
	}

	@Test
	public void setNewElementCollectionWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							context.assertNotNull( foundPerson );
							context.assertFalse( foundPerson.getPhones().isEmpty() );
							foundPerson.setPhones( Arrays.asList( "555" ) );
						} )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( changedPerson -> assertPhones( context, changedPerson, "555" ) ) )
		);
	}

	@Test
	public void removePersonWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( foundPerson -> session.remove( foundPerson ) )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( nullPerson -> context.assertNull( nullPerson ) ) )
				// Check with native query that the table is empty
				.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
				.thenAccept( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void removePersonWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.call( session::remove )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( nullPerson -> context.assertNull( nullPerson ) ) )
				// Check with native query that the table is empty
				.chain( () -> selectFromPhonesWithMutiny( thePerson ) )
				.invoke( resultList -> context.assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, secondPerson.getId() )
						.thenAccept( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333" ) ) ) )
				// Check initial person collection hasn't changed
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.thenAccept( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( "222-222-2222", "333-333-3333" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, secondPerson.getId() ) )
						.invoke( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333" ) ) )
				// Check initial person collection hasn't changed
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( foundPerson -> assertPhones( context, foundPerson, "999-999-9999", "111-111-1111", "123-456-7890" ) ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, secondPerson.getId() )
						// Null values don't get persisted
						.thenAccept( foundPerson -> context.assertTrue( foundPerson.getPhones().isEmpty() ) ) ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, secondPerson.getId() )
						// Null values don't get persisted
						.invoke( foundPerson -> context.assertTrue( foundPerson.getPhones().isEmpty() ) ) ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.thenCompose( v -> getSessionFactory()
						.withSession(session -> session.find( Person.class, secondPerson.getId() )
						// Null values don't get persisted
						.thenAccept( foundPerson -> assertPhones( context, foundPerson, "567" ) ) ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(TestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, "567", null ) );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( secondPerson ) )
				// Check new person collection
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, secondPerson.getId() ) )
						// Null values don't get persisted
						.invoke( foundPerson -> assertPhones( context, foundPerson, "567" ) ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							context.assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
				)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> assertPhones( context, foundPerson ) ) ) )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( found -> {
							context.assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, thePerson.getId() ) )
						.invoke( foundPerson -> assertPhones( context, foundPerson ) ) )
		);
	}

	/**
	 * With the Stage API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person.
	 */
	private CompletionStage<List<Object>> selectFromPhonesWithStage(Person person) {
		return getSessionFactory().withTransaction( (session, transaction) -> session
				.createNativeQuery( "SELECT * FROM Person_phones where Person_id = ?" )
				.setParameter( 1, person.getId() )
				.getResultList() );
	}

	/**
	 * With the Mutiny API, run a native query to check the content of the table containing the collection of elements
	 * associated to the selected person
	 */
	private Uni<List<Object>> selectFromPhonesWithMutiny(Person person) {
		return getMutinySessionFactory().withTransaction( (session, transaction) -> session
				.createNativeQuery( "SELECT * FROM Person_phones where Person_id = ?" )
				.setParameter( 1, person.getId() )
				.getResultList() );
	}

	private static void assertPhones(TestContext context, Person person, String... expectedPhones) {
		context.assertNotNull( person );
		assertThat( person.getPhones() ).containsExactlyInAnyOrder( expectedPhones );
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

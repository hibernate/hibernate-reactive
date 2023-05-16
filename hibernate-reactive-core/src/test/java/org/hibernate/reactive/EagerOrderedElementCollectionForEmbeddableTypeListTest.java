/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.OrderBy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class EagerOrderedElementCollectionForEmbeddableTypeListTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	private Person getPerson() {
		if( thePerson == null ) {
			List<Phone> phones = new ArrayList<>();
			phones.add( new Phone( "999-999-9999" ) );
			phones.add( new Phone( "111-111-1111" ) );
			thePerson = new Person( 777777, "Claude", phones );
		}
		return thePerson;
	}

	private Uni<Void> populateDbMutiny() {
		return getMutinySessionFactory().withTransaction( (s, t) -> s.persist( getPerson() ) );
	}

	private CompletionStage<Void> populateDb() {
		return getSessionFactory().withTransaction( s -> s.persist(  getPerson() ) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "888" ) );
		phones.add( new Phone( "555" ) );
		Person johnny = new Person( 999, "Johnny English", phones );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( johnny )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, johnny.getId() ) )
				.invoke( found -> assertPhones( context, found, "555", "888" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) ))
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "111-111-1111", "999-999-9999" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "111-111-1111", "999-999-9999" ) ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().add( new Phone( "000" ) ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "000", "111-111-1111", "999-999-9999" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		Person thomas = new Person( 7, "Thomas Reaper", phones );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( thomas )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		phones.add( new Phone( "111" ) );
		Person thomas = new Person( 567, "Thomas Reaper", phones );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( thomas )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( context, found, "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.persist( thomas ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.thenAccept( found -> {
							found.getPhones().set( 1, new Phone( "47" ) );
							found.getPhones().set( 3, new Phone( "47" ) );
						} )
						.thenCompose( ignore -> session.flush() ) )
				.thenCompose( ignore -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "000", "000", "47", "47" ) )

		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.persist( thomas ) )
				.chain( this::openMutinySession )
				.chain( newSession -> newSession
						.find( Person.class, thomas.getId() )
						// Change a couple of the elements in the collection
						.invoke( found -> {
							found.getPhones().set( 1, new Phone( "47" ) );
							found.getPhones().set( 3, new Phone( "47" ) );
						} )
						.call( newSession::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( context, found, "000", "000", "47", "47" ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.persist( thomas ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( newSession -> newSession
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.thenAccept( found -> {
							// it doesn't matter which elements are deleted because they are all equal
							found.getPhones().remove( 1 );
							found.getPhones().remove( 2 );
						} )
						.thenCompose( ignore -> newSession.flush() ) )
				.thenCompose( ignore -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "000", "000" ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );
		phones.add( new Phone( "000" ) );

		Person thomas = new Person( 47, "Thomas Reaper", phones );

		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.persist( thomas ) )
				.chain( this::openMutinySession )
				.chain( newSession -> newSession
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.invoke( found -> {
							// it doesn't matter which elements are deleted because they are all equal
							found.getPhones().remove( 1 );
							found.getPhones().remove( 2 );
						} )
						.call( newSession::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( context, found, "000", "000" ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.invoke( foundPerson -> foundPerson.getPhones().add( new Phone( "000" ) ) ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( updatedPerson -> assertPhones( context, updatedPerson, "000", "111-111-1111", "999-999-9999" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhones().remove( new Phone( "999-999-9999" ) ) ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "111-111-1111" ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhones().remove( new Phone( "999-999-9999" ) ) ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "111-111-1111" ) )
		);
	}

	@Test
	public void clearCollectionElementsStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// clear collection
						.thenAccept( foundPerson -> foundPerson.getPhones().clear() ) ) )
				.thenCompose( s -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson ) )
		);
	}

	@Test
	public void clearCollectionElementsMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// clear collection
						.invoke( foundPerson -> foundPerson.getPhones().clear() ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() )
						.invoke( changedPerson -> assertTrue( changedPerson.getPhones().isEmpty() ) ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( new Phone( "111-111-1111" ) );
							foundPerson.getPhones().add( new Phone( "000" ) );
						} ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "999-999-9999" ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(VertxTestContext context){
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhones().remove( new Phone( "111-111-1111" ) );
							foundPerson.getPhones().add( new Phone( "000" ) );
						} ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( person -> assertPhones( context, person, "000", "999-999-9999" ) )
		);
	}

	@Test
	public void replaceSecondCollectionElementStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> {
							// remove existing phone and add new phone
							foundPerson.getPhones().remove( new Phone( "999-999-9999" ) );
							foundPerson.getPhones().add( new Phone( "000-000-0000" ) );
						} ) ) )
				.thenCompose( s -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000-000-0000", "111-111-1111" ) )
		);
	}

	@Test
	public void replaceSecondCollectionElementMutinyAPI(VertxTestContext context){
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							// remove existing phone and add new phone
							foundPerson.getPhones().remove( new Phone( "999-999-9999" ) );
							foundPerson.getPhones().add( new Phone( "000-000-0000" ) );
						} ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "000-000-0000", "111-111-1111" ) )
		);
	}

	@Test
	public void setNewElementCollectionStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// replace phones with list of 1 phone
						.thenAccept( foundPerson -> foundPerson.setPhones( List.of( new Phone( "000-000-0000" ) ) ) ) ) )
				.thenCompose( s -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000-000-0000" ) )
		);
	}

	@Test
	public void setNewElementCollectionMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// replace phones with list of 1 phone
						.invoke( foundPerson -> foundPerson.setPhones( List.of( new Phone( "000-000-0000" ) ) ) ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "000-000-0000" ) )
		);
	}

	@Test
	public void removePersonStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.thenCompose( session::remove ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( Assertions::assertNull )
				// Check with native query that the table is empty
				.thenCompose( v -> selectFromPhonesWithStage( thePerson ) )
				.thenAccept( resultList -> assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void removePersonMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.call( session::remove ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( Assertions::assertNull )
				// Check with native query that the table is empty
				.chain( () -> selectFromPhonesWithMutiny( thePerson ) )
				.invoke( resultList -> assertTrue( resultList.isEmpty() ) )
		);
	}

	@Test
	public void persistAnotherPersonWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty",
										  Arrays.asList(
												  new Phone( "222-222-2222" ),
												  new Phone( "333-333-3333" ),
												  new Phone( "444-444-4444" )
										  )
		);

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333", "444-444-4444" ) )
				// Check initial person collection hasn't changed
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "111-111-1111", "999-999-9999" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty",
										  Arrays.asList(
												  new Phone( "222-222-2222" ),
												  new Phone( "333-333-3333" ),
												  new Phone( "444-444-4444" )
										  )
		);

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "222-222-2222", "333-333-3333", "444-444-4444" ) )
				// Check initial person collection hasn't changed
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "111-111-1111", "999-999-9999" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, null ) );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertTrue( foundPerson.getPhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, new Phone( "567" ), null ) );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				// Check new person collection
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(VertxTestContext context) {
		Person secondPerson = new Person( 9910000, "Kitty", Arrays.asList( null, new Phone( "567" ), null ) );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertPhones( context, foundPerson, "567" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson ) )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						.invoke( found -> {
							assertFalse( found.getPhones().isEmpty() );
							found.setPhones( null );
						} )
						.call( session::flush ) ) )
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

	private static void assertPhones(VertxTestContext context, Person person, String... phones) {
		assertNotNull( person );
		assertEquals( phones.length, person.getPhones().size() );
		for (int i=0; i<phones.length; i++) {
			assertEquals( phones[i], person.getPhones().get(i).getNumber() );
		}
	}

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		@OrderBy("country, number")
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

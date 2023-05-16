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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
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

@Timeout( value = 5, timeUnit = TimeUnit.MINUTES )
public class EagerElementCollectionForEmbeddedEmbeddableTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	private Person getPerson() {
		if( thePerson == null ) {
			Phone phone = new Phone(MOBILE, "000");
			phone.addAlternatePhone( HOME, "111" );
			phone.addAlternatePhone( WORK, "222" );
			thePerson = new Person( 999, "Johnny English", phone );
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
		Phone phone = new Phone(MOBILE, "444");
		phone.addAlternatePhone( HOME, "888" );
		phone.addAlternatePhone( WORK, "555" );

		Person johnny = new Person( 999, "Johnny English", phone );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( johnny )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, johnny.getId() ) )
				.invoke( found -> assertPhones( context, found, "444", "888", "555" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void findEntityWithElementCollectionMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson,"000", "111", "222" ) ) )
		);
	}

	@Test
	public void addOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhone()
								.getAlternativePhones()
								.add( new AlternativePhone( MOBILE, "333" ) ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( updatedPerson -> assertPhones( context, updatedPerson, "000", "111", "222", "333" ) )
		);
	}

	@Test
	public void addOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// add one element to the collection
						.invoke( foundPerson -> foundPerson.getPhone()
								.getAlternativePhones()
								.add( new AlternativePhone( MOBILE, "333" ) ) )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( updatedPerson -> assertPhones( context, updatedPerson, "000", "111", "222", "333" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );

		Person thomas = new Person( 7, "Thomas Reaper", phone );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( thomas )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "555", "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void persistCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );
		phone.addAlternatePhone( HOME, "111" );

		Person thomas = new Person( 567, "Thomas Reaper", phone );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( thomas )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( context, found, "555", "111", "111", "111", "111" ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( thomas )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.thenAccept( found -> {
							found.getPhone().getAlternativePhones().set( 1, new AlternativePhone( MOBILE, "47" ) );
							found.getPhone().getAlternativePhones().set( 3, new AlternativePhone( MOBILE, "47" ) );
						} )
						.thenCompose( ignore -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "555", "000", "47", "000", "47" ) )
		);
	}

	@Test
	public void updateCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( thomas )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() )
						// Change a couple of the elements in the collection
						.invoke( found -> {
							found.getPhone().getAlternativePhones().set( 1, new AlternativePhone( MOBILE, "47" ) );
							found.getPhone().getAlternativePhones().set( 3, new AlternativePhone( MOBILE, "47" ) );
						} )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( context, found, "555", "000", "47", "000", "47" ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithStageAPI(VertxTestContext context) {
		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.persist( thomas )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.thenAccept( found -> {
							// it doesn't matter which elements are deleted because they are all equal
							found.getPhone().getAlternativePhones().remove( 1 );
							found.getPhone().getAlternativePhones().remove( 2 );
						} )
						.thenCompose( ignore -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thomas.getId() ) )
				.thenAccept( found -> assertPhones( context, found, "555", "000", "000" ) )
		);
	}

	@Test
	public void deleteElementsFromCollectionWithDuplicatesWithMutinyAPI(VertxTestContext context) {
		Phone phone = new Phone(MOBILE, "555");
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );
		phone.addAlternatePhone( HOME, "000" );

		Person thomas = new Person( 47, "Thomas Reaper", phone );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( thomas )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() )
						// Change one of the element in the collection
						.invoke( found -> {
							// it doesn't matter which elements are deleted because they are all equal
							found.getPhone().getAlternativePhones().remove( 1 );
							found.getPhone().getAlternativePhones().remove( 2 );
						} )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thomas.getId() ) )
				.invoke( found -> assertPhones( context, found, "555", "000", "000" ) )
		);
	}

	@Test
	public void removeOneElementWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.thenAccept( foundPerson -> foundPerson.getPhone()
								.getAlternativePhones()
								.remove( new AlternativePhone( MOBILE, "222" ) ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111" ) ) )
		);
	}

	@Test
	public void removeOneElementWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// Remove one element from the collection
						.invoke( foundPerson -> foundPerson.getPhone()
								.getAlternativePhones()
								.remove( new AlternativePhone( MOBILE, "222" ) ) )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "000", "111" ) )
		);
	}

	@Test
	public void clearCollectionElementsStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// clear collection
						.thenAccept( foundPerson -> foundPerson.getPhone().getAlternativePhones().clear() )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000" ) )
		);
	}

	@Test
	public void clearCollectionElementsMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// clear collection
						.invoke( foundPerson -> foundPerson.getPhone().getAlternativePhones().clear() )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "000" ) )
		);
	}

	@Test
	public void removeAndAddElementWithStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE, "111") );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444") );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "222", "444" ) )
		);
	}

	@Test
	public void removeAndAddElementWithMutinyAPI(VertxTestContext context){
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						.invoke( foundPerson -> {
							assertNotNull( foundPerson );
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE, "111" ) );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444" ) );
						} )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( person -> assertPhones( context, person, "000", "222", "444" ) )
		);
	}

	@Test
	public void replaceSecondCollectionElementStageAPI(VertxTestContext context){
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId())
						.thenAccept( foundPerson -> {
							// remove existing phone and add new phone
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE,  "222" ) );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444" ) );
						} )
						.thenCompose(v -> session.flush()) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "111", "444" ) )
		);
	}

	@Test
	public void replaceSecondCollectionElementMutinyAPI(VertxTestContext context){
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId())
						.invoke( foundPerson -> {
							// remove existing phone and add new phone
							foundPerson.getPhone().getAlternativePhones().remove( new AlternativePhone( MOBILE,  "222" ) );
							foundPerson.getPhone().getAlternativePhones().add( new AlternativePhone( MOBILE, "444" ) );
						} )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "000", "111", "444" ) )
		);
	}

	@Test
	public void setNewElementCollectionStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() )
						// replace phones with list of 1 phone
						.thenAccept( foundPerson -> foundPerson.getPhone()
								.setAlternativePhones( List.of( new AlternativePhone( MOBILE, "555" ) ) ) )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( changedPerson -> assertPhones( context, changedPerson, "000", "555" ) )
		);
	}

	@Test
	public void setNewElementCollectionMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId())
						// replace phones with list of 1 phone
						.invoke( foundPerson -> foundPerson.getPhone()
								.setAlternativePhones( List.of( new AlternativePhone( MOBILE, "555" ) ) ) )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( changedPerson -> assertPhones( context, changedPerson, "000", "555" ) )
		);
	}

	@Test
	public void removePersonStageAPI(VertxTestContext context) {
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
	public void removePersonMutinyAPI(VertxTestContext context){
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						// remove thePerson entity and flush
						.call( session::remove )
						.call( session::flush ) ) )
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
		Phone phone = new Phone( MOBILE, "999");
		Person secondPerson = new Person( 9910000, "Kitty", phone);
		phone.setAlternativePhones(
				Arrays.asList(
					  new AlternativePhone( HOME, "222" ),
					  new AlternativePhone( WORK, "333" ),
					  new AlternativePhone( MOBILE, "444" )
			  )
		);

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "999", "222", "333", "444" ) )
				// Check initial person collection hasn't changed
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void persistAnotherPersonWithMutinyAPI(VertxTestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		Person secondPerson = new Person( 9910000, "Kitty", phone);
		phone.setAlternativePhones(
				Arrays.asList(
						new AlternativePhone( HOME, "222" ),
						new AlternativePhone( WORK, "333" ),
						new AlternativePhone( MOBILE, "444" )
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
				.invoke( foundPerson -> assertPhones( context, foundPerson, "999", "222", "333", "444" ) )
				// Check initial person collection hasn't changed
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "000", "111", "222" ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithStageAPI(VertxTestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertTrue( foundPerson.getPhone().getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionOfNullsWithMutinyAPI(VertxTestContext context) {
		Phone phone = new Phone( MOBILE, "999" );
		phone.setAlternativePhones( Arrays.asList( null, null ) );

		Person secondPerson = new Person( 9910000, "Kitty", phone );

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertTrue( foundPerson.getPhone().getAlternativePhones().isEmpty() ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithStageAPI(VertxTestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, new AlternativePhone( HOME, "222" ), null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.persist( secondPerson )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "999", "222" ) )
		);
	}

	@Test
	public void persistCollectionWithNullsWithMutinyAPI(VertxTestContext context) {
		Phone phone = new Phone( MOBILE, "999");
		phone.setAlternativePhones( Arrays.asList(null, new AlternativePhone( HOME, "222" ), null) );

		Person secondPerson = new Person( 9910000, "Kitty", phone);

		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.persist( secondPerson )
						.call( session::flush ) ) )
				// Check new person collection
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, secondPerson.getId() ) )
				// Null values don't get persisted
				.invoke( foundPerson -> assertPhones( context, foundPerson, "999", "222" ) )
		);
	}

	@Test
	public void setCollectionToNullWithStageAPI(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.find( Person.class, thePerson.getId() )
						.thenAccept( found -> {
							assertFalse( found.getPhone().getAlternativePhones().isEmpty() );
							found.getPhone().setAlternativePhones( null );
						} )
						.thenCompose( v -> session.flush() ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( foundPerson -> assertPhones( context, foundPerson, "000" ) )
		);
	}

	@Test
	public void setCollectionToNullWithMutinyAPI(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session
						.find( Person.class, thePerson.getId() )
						.invoke( found -> {
							assertFalse( found.getPhone().getAlternativePhones().isEmpty() );
							found.getPhone().setAlternativePhones( null );
						} )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Person.class, thePerson.getId() ) )
				.invoke( foundPerson -> assertPhones( context, foundPerson, "000" ) )
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

	private static void assertPhones(VertxTestContext context, Person person, String mainPhone, String... phones) {
		assertNotNull( person );
		assertNotNull( person.getPhone() != null );
		assertEquals( mainPhone, person.getPhone().getNumber() );
		if( phones != null ) {
			assertEquals( phones.length, person.getPhone().getAlternativePhones().size() );
			for ( String number : phones ) {
				assertTrue( phoneContainsNumber( person.getPhone(), number ) );
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

	private static final String MOBILE = "mobile";
	private static final String HOME = "home";
	private static final String WORK = "work";

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

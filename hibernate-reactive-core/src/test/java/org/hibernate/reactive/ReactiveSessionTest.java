/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.LockMode;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;
import javax.persistence.Table;
import javax.persistence.Version;
import javax.persistence.metamodel.EntityType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveSessionTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		configuration.setProperty(AvailableSettings.STATEMENT_BATCH_SIZE, "5");
		return configuration;
	}

	private CompletionStage<Void> populateDB() {
		return getSessionFactory()
				.withTransaction( (s, tx) -> s.persist( new GuineaPig( 5, "Aloi" ) ) );
	}

	@After
	public void cleanDB(TestContext context) {
		test( context, deleteEntities( "GuineaPig" ) );
	}

	private CompletionStage<String> selectNameFromId(Integer id) {
		return getSessionFactory().withSession(
				session -> session.createQuery("SELECT name FROM GuineaPig WHERE id = " + id )
						.getResultList()
						.thenApply( ReactiveSessionTest::nameFromResult )
		);
	}

	private static String nameFromResult(List<Object> rowSet) {
		switch ( rowSet.size() ) {
			case 0:
				return null;
			case 1:
				return (String) rowSet.get( 0 );
			default:
				throw new AssertionError( "More than one result returned: " + rowSet.size() );
		}
	}

	@Test
	public void reactiveFind(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertTrue( session.contains( actualPig ) );
									context.assertFalse( session.contains( expectedPig ) );
									context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
									session.detach( actualPig );
									context.assertFalse( session.contains( actualPig ) );
								} )
						)
		);
	}

	@Test
	public void reactivePersistFindDelete(TestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		Stage.Session session = getSessionFactory().openSession();
		test(
				context,
				session.persist( guineaPig )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.detach(guineaPig) )
						.thenAccept( v -> context.assertFalse( session.contains(guineaPig) ) )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( context, guineaPig, actualPig );
							context.assertTrue( session.contains( actualPig ) );
							context.assertFalse( session.contains( guineaPig ) );
							context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
							session.detach( actualPig );
							context.assertFalse( session.contains( actualPig ) );
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() )
		);
	}

	@Test
	public void reactiveFindWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );

		test( context, populateDB().thenCompose( v -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( context, expectedPig, actualPig );
							context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
						} )
				) )
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( v -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.refresh( pig, LockMode.PESSIMISTIC_WRITE )
										.thenAccept( vv -> {
											assertThatPigsAreEqual( context, expectedPig, pig );
											context.assertEquals(session.getLockMode( pig ), LockMode.PESSIMISTIC_WRITE );
										} )
								)
						) )
		);
	}

	@Test
	public void reactiveFindReadOnlyRefreshWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> {
									session.setReadOnly(pig, true);
									pig.setName("XXXX");
									return session.flush()
											.thenCompose( v -> session.refresh(pig) )
											.thenAccept( v -> {
												context.assertEquals(expectedPig.name, pig.name);
												context.assertEquals(true, session.isReadOnly(pig));
											} );
								} )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> {
									session.setReadOnly(pig, false);
									pig.setName("XXXX");
									return session.flush()
											.thenCompose( v -> session.refresh(pig) )
											.thenAccept( v -> {
												context.assertEquals("XXXX", pig.name);
												context.assertEquals(false, session.isReadOnly(pig));
											} );
								} )
						)
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( unused -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session
										.lock( pig, LockMode.PESSIMISTIC_READ )
										.thenAccept( v -> {
											assertThatPigsAreEqual( context, expectedPig, pig );
											context.assertEquals( session.getLockMode( pig ), LockMode.PESSIMISTIC_READ );
										} )
								)
						)
				)
		);
	}

	@Test
	public void reactiveFindThenWriteLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB().thenCompose( v -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( GuineaPig.class, expectedPig.getId() )
						.thenCompose( pig -> session
								.lock( pig, LockMode.PESSIMISTIC_WRITE )
								.thenAccept( vv -> {
									assertThatPigsAreEqual( context, expectedPig, pig );
									context.assertEquals( session.getLockMode( pig ), LockMode.PESSIMISTIC_WRITE );
									context.assertEquals( pig.version, 0 );
								} )
						)
				) )
		);
	}

	@Test
	public void reactiveFindThenForceLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock(pig, LockMode.PESSIMISTIC_FORCE_INCREMENT).thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_FORCE_INCREMENT );
									context.assertEquals( actualPig.version, 1 );
								} )
								.thenCompose( v -> session.createQuery("select version from GuineaPig").getSingleResult() )
								.thenAccept( version -> context.assertEquals(1, version) )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock(pig, LockMode.PESSIMISTIC_FORCE_INCREMENT).thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_FORCE_INCREMENT );
									context.assertEquals( actualPig.version, 2 );
								} )
								.thenCompose( v -> session.createQuery("select version from GuineaPig").getSingleResult() )
								.thenAccept( version -> context.assertEquals(2, version) )
						)
		);
	}

	@Test
	public void reactiveFindWithPessimisticIncrementLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_FORCE_INCREMENT )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.FORCE ); //grrr, lame
											context.assertEquals( 1, actualPig.version );
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 1, actualPig.version ) )
		);
	}

	@Test
	public void reactiveFindWithOptimisticIncrementLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.OPTIMISTIC_FORCE_INCREMENT )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.OPTIMISTIC_FORCE_INCREMENT );
											context.assertEquals( 0, actualPig.version );
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 1, actualPig.version ) )
		);
	}

	@Test
	public void reactiveLockWithOptimisticIncrement(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.OPTIMISTIC_FORCE_INCREMENT )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( context, expectedPig, actualPig );
													context.assertEquals( session.getLockMode( actualPig ), LockMode.OPTIMISTIC_FORCE_INCREMENT );
													context.assertEquals( 0, actualPig.version );
												} )
										)
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 1, actualPig.version ) )
		);
	}

	@Test
	public void reactiveLockWithIncrement(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.PESSIMISTIC_FORCE_INCREMENT )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( context, expectedPig, actualPig );
													context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_FORCE_INCREMENT );
													context.assertEquals( 1, actualPig.version );
												} )
										)
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 1, actualPig.version ) )
		);
	}

	@Test
	public void reactiveFindWithOptimisticVerifyLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.OPTIMISTIC )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.OPTIMISTIC );
											context.assertEquals( 0, actualPig.version );
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveLockWithOptimisticVerify(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.OPTIMISTIC )
											.thenAccept( vv -> {
												assertThatPigsAreEqual( context, expectedPig, actualPig );
												context.assertEquals( session.getLockMode( actualPig ), LockMode.OPTIMISTIC );
												context.assertEquals( 0, actualPig.version );
											} )
										)
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveFindWithPessimisticRead(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								// does a select ... for share
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_READ )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
											context.assertEquals( 0, actualPig.version );
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveLockWithPessimisticRead(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								// does a select ... for share
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.PESSIMISTIC_READ )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( context, expectedPig, actualPig );
													context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
													context.assertEquals( 0, actualPig.version );
												} )
										)
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveFindWithPessimisticWrite(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								// does a select ... for update
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
											context.assertEquals( 0, actualPig.version );
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveLockWithPessimisticWrite(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								// does a select ... for update
								(session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.PESSIMISTIC_WRITE )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( context, expectedPig, actualPig );
													context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
													context.assertEquals( 0, actualPig.version );
												} )
										)
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> context.assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveQueryWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
										.setLockMode(LockMode.PESSIMISTIC_WRITE)
										.getSingleResult()
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
								)
						)
		);
	}

	@Test
	public void reactiveQueryWithAliasedLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose(
								v -> getSessionFactory().withTransaction(
										(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
												.setLockMode("pig", LockMode.PESSIMISTIC_WRITE )
												.getSingleResult()
												.thenAccept( actualPig -> {
													assertThatPigsAreEqual( context, expectedPig, actualPig );
													context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
												} )
								)
						)
		);
	}

	@Test
	public void reactivePersist(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( new GuineaPig( 10, "Tulip" ) )
								.thenCompose( v -> s.flush() )
								.thenCompose( v -> s.close() )
						)
						.thenCompose( v -> selectNameFromId( 10 ) )
						.thenAccept( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersistInTx(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(
								s -> s.withTransaction( t -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
										.thenCompose( v -> s.close() )
						)
						.thenCompose( vv -> selectNameFromId( 10 ) )
						.thenAccept( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRollbackTx(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
												.thenCompose( v -> s.flush() )
												.thenAccept( v -> { throw new RuntimeException(); } )
								)
										.thenCompose( v -> s.close() )
						)
						.handle( (v, e) -> null )
						.thenCompose( vv -> selectNameFromId( 10 ) )
						.thenAccept( context::assertNull )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
												.thenCompose( vv -> s.flush() )
												.thenAccept( vv -> t.markForRollback() )
								)
										.thenCompose( v -> s.close() )
						)
						.thenCompose( vv -> selectNameFromId( 10 ) )
						.thenAccept( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity(TestContext context) {
		test(
				context,
				populateDB()
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( context::assertNotNull )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.remove( new GuineaPig( 5, "Aloi" ) )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( context::assertNull )
						.handle((r, e) -> context.assertNotNull(e))
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(TestContext context) {
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session ->
							session.find( GuineaPig.class, 5 )
								.thenCompose( session::remove )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> selectNameFromId( 5 ) )
								.thenAccept( context::assertNull ) )
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, 5 )
								.thenAccept( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( name -> context.assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveUpdateVersion(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, 5 )
								.thenAccept( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									context.assertEquals( pig.version, 0 );
									pig.setName( NEW_NAME );
									pig.version = 10; //ignored by Hibernate
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( GuineaPig.class, 5 )
								.thenAccept( pig -> context.assertEquals( pig.version, 1 ) ) )
		);
	}

	@Test
	public void reactiveClose(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> {
					context.assertTrue( session.isOpen() );
					return session.close()
							.thenAccept( v -> context.assertFalse( session.isOpen() ) );
				} )
		);
	}

	@Test
	public void testBatching(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist( new GuineaPig(11, "One") ) )
								.thenCompose( v -> s.persist( new GuineaPig(22, "Two") ) )
								.thenCompose( v -> s.persist( new GuineaPig(33, "Three") ) )
								.thenCompose( v -> s.createQuery("select count(*) from GuineaPig")
										.getSingleResult()
										.thenAccept( count -> context.assertEquals( 3L, count) )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.<GuineaPig>createQuery("from GuineaPig")
								.getResultList()
								.thenAccept( list -> list.forEach( pig -> pig.setName("Zero") ) )
								.thenCompose( v -> s.<Long>createQuery("select count(*) from GuineaPig where name='Zero'")
										.getSingleResult()
										.thenAccept( count -> context.assertEquals( 3L, count) )
								) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.<GuineaPig>createQuery("from GuineaPig")
								.getResultList()
								.thenAccept( list -> list.forEach(s::remove) )
								.thenCompose( v -> s.<Long>createQuery("select count(*) from GuineaPig")
										.getSingleResult()
										.thenAccept( count -> context.assertEquals( 0L, count) )
								)
						)
		);
	}

	@Test
	public void testSessionWithNativeAffectedEntities(TestContext context) {
		GuineaPig pig = new GuineaPig(3,"Rorshach");
		AffectedEntities affectsPigs = new AffectedEntities(GuineaPig.class);
		Stage.Session s = getSessionFactory().openSession();
		test(
				context,
				s.persist(pig)
						.thenCompose( v -> s.createNativeQuery("select * from pig where name=:n", GuineaPig.class, affectsPigs)
								.setParameter("n", pig.name)
								.getResultList() )
						.thenAccept( list -> {
							context.assertFalse( list.isEmpty() );
							context.assertEquals(1, list.size());
							assertThatPigsAreEqual(context, pig, list.get(0));
						} )
						.thenCompose( v -> s.find(GuineaPig.class, pig.id) )
						.thenAccept( p -> {
							assertThatPigsAreEqual(context, pig, p);
							p.name = "X";
						} )
						.thenCompose( v -> s.createNativeQuery("update pig set name='Y' where name='X'", affectsPigs).executeUpdate() )
						.thenAccept( rows -> context.assertEquals(1, rows) )
						.thenCompose( v -> s.refresh(pig) )
						.thenAccept( v -> context.assertEquals(pig.name, "Y") )
						.thenAccept( v -> pig.name = "Z" )
						.thenCompose( v -> s.createNativeQuery("delete from pig where name='Z'", affectsPigs).executeUpdate() )
						.thenAccept( rows -> context.assertEquals(1, rows) )
						.thenCompose( v -> s.createNativeQuery("select id from pig").getResultList() )
						.thenAccept( list -> context.assertTrue( list.isEmpty() ) )
		);
	}

	@Test
	public void testMetamodel(TestContext context) {
		EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity(GuineaPig.class);
		context.assertNotNull(pig);
		context.assertEquals( 3, pig.getAttributes().size() );
		context.assertEquals( "GuineaPig", pig.getName() );
	}

	@Test
	public void testTransactionPropagation(TestContext context) {
		test( context, getSessionFactory().withTransaction(
				(session, transaction) -> session.createQuery("from GuineaPig").getResultList()
						.thenCompose( list -> {
							context.assertNotNull( session.currentTransaction() );
							context.assertFalse( session.currentTransaction().isMarkedForRollback() );
							session.currentTransaction().markForRollback();
							context.assertTrue( session.currentTransaction().isMarkedForRollback() );
							context.assertTrue( transaction.isMarkedForRollback() );
							return session.withTransaction( t -> {
								context.assertTrue( t.isMarkedForRollback() );
								return session.createQuery("from GuineaPig").getResultList();
							} );
						} )
		) );
	}

	@Test
	public void testSessionPropagation(TestContext context) {
		test( context, getSessionFactory().withSession( session -> {
			context.assertEquals( false, session.isDefaultReadOnly() );
			session.setDefaultReadOnly(true);
			return session.createQuery("from GuineaPig").getResultList()
					.thenCompose( list -> getSessionFactory().withSession(s -> {
						context.assertEquals( true, s.isDefaultReadOnly() );
						return s.createQuery("from GuineaPig").getResultList();
					} ) );
		} ) );
	}

	@Test
	public void testDupeException(TestContext context) {
		test(
				context,
				getSessionFactory()
						.withTransaction((s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) ))
						.thenCompose(v -> getSessionFactory()
								.withTransaction((s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) ))
						).handle((i, t) -> {
					context.assertNotNull(t);
					context.assertTrue(t instanceof CompletionException);
					context.assertTrue(t.getCause() instanceof PersistenceException);
					return null;
				})
		);
	}

	@Test
	public void testExceptionInWithSession(TestContext context) {
		final Stage.Session[] savedSession = new Stage.Session[1];
		test( context, getSessionFactory().withSession( session -> {
			context.assertTrue( session.isOpen() );
			savedSession[0] = session;
			throw new RuntimeException( "No Panic: This is just a test" );
		} ).handle( (o, t) -> {
			context.assertNotNull( t );
			context.assertFalse( savedSession[0].isOpen(), "Session should be closed" );
			return null;
		} ) );
	}

	@Test
	public void testExceptionInWithTransaction(TestContext context) {
		final Stage.Session[] savedSession = new Stage.Session[1];
		test( context, getSessionFactory().withTransaction( (session, tx) -> {
			context.assertTrue( session.isOpen() );
			savedSession[0] = session;
			throw new RuntimeException( "No Panic: This is just a test" );
		} ).handle( (o, t) -> {
			context.assertNotNull( t );
			context.assertFalse( savedSession[0].isOpen(), "Session should be closed" );
			return null;
		} ) );
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
	}

	@Entity(name="GuineaPig")
	@Table(name="pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;
		@Version
		private int version;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
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

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}

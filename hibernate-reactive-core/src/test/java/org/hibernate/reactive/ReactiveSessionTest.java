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
import org.hibernate.reactive.stage.Stage;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;
import javax.persistence.metamodel.EntityType;
import java.util.Objects;
import java.util.concurrent.CompletionStage;


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
				.withSession(
						session -> session.persist( new GuineaPig(5, "Aloi") )
								.thenApply( v -> { session.flush(); return null; } )
				);
	}

	private CompletionStage<Integer> cleanDB() {
		return getSessionFactory()
				.withSession( session -> session.createQuery( "delete GuineaPig" ).executeUpdate() );
	}

	public void after(TestContext context) {
		test( context,
			  cleanDB()
				.whenComplete( (res, err) -> {
					// in case cleanDB() fails we
					// still have to close the factory
					super.after( context );
				} )
		);
	}

	private CompletionStage<String> selectNameFromId(Integer id) {
		return getSessionFactory().withSession(
				session -> session.createQuery("SELECT name FROM GuineaPig WHERE id = " + id )
						.getResultList()
						.thenApply(
								rowSet -> {
									switch ( rowSet.size() ) {
										case 0:
											return null;
										case 1:
											return (String) rowSet.get(0);
										default:
											throw new AssertionError("More than one result returned: " + rowSet.size());
									}
								}
						)
		);
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
								.whenComplete( (v, err) -> session.close() )
						)
		);
	}

	@Test
	public void reactivePersistFindDelete(TestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		Stage.Session session = getSessionFactory().createSession();
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
						.thenCompose( pig -> session.remove(pig) )
						.thenCompose( v -> session.flush() )
						.whenComplete( (v, err) -> session.close() )
		);
	}

	@Test
	public void reactiveFindWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
								} )
								.whenComplete( (v, err) -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.refresh(pig, LockMode.PESSIMISTIC_WRITE).thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
								} )
								.whenComplete( (v, err) -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock(pig, LockMode.PESSIMISTIC_READ).thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
								} )
								.whenComplete( (v, err) -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindThenWriteLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock(pig, LockMode.PESSIMISTIC_WRITE).thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
									context.assertEquals( actualPig.version, 0 );
								} )
								.whenComplete( (v, err) -> session.close() )
						)
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
								.whenComplete( (v, err) -> session.close() )
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
								.whenComplete( (v, err) -> session.close() )
						)
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
						.thenCompose( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.thenCompose( s -> s.flush() )
						.whenComplete( (s,e) -> s.close() )
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
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
								)
										.whenComplete( (v,e) -> s.close() )
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
										.whenComplete( (v,e) -> s.close() )
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
										.whenComplete( (v,e) -> s.close() )
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
						.thenCompose( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.thenCompose( session -> session.flush() )
						.whenComplete( (session, err) -> session.close() )
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( context::assertNull )
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
								.thenCompose( aloi -> session.remove( aloi ) )
								.thenCompose( v -> session.flush() )
								.whenComplete( (v,e) -> session.close() )
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
								.whenComplete( (v,e) -> session.close() )
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
								.whenComplete( (v,e) -> session.close() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( GuineaPig.class, 5 )
								.thenAccept( pig -> context.assertEquals( pig.version, 1 ) ) )
		);
	}

	@Test
	public void testBatching(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( new GuineaPig(11, "One") ) )
						.thenCompose( s -> s.persist( new GuineaPig(22, "Two") ) )
						.thenCompose( s -> s.persist( new GuineaPig(33, "Three") ) )
						.thenCompose( s -> s.<Long>createQuery("select count(*) from GuineaPig")
								.getSingleResult()
								.thenAccept( count -> context.assertEquals(3l, count) )
								.thenAccept( v -> s.close() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.<GuineaPig>createQuery("from GuineaPig")
								.getResultList()
								.thenAccept( list -> list.forEach( pig -> pig.setName("Zero") ) )
								.thenCompose( v -> s.<Long>createQuery("select count(*) from GuineaPig where name='Zero'")
										.getSingleResult()
										.thenAccept( count -> context.assertEquals(3l, count) )
										.thenAccept( vv -> s.close() )
								) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.<GuineaPig>createQuery("from GuineaPig")
								.getResultList()
								.thenAccept( list -> list.forEach(s::remove) )
								.thenCompose( v -> s.<Long>createQuery("select count(*) from GuineaPig")
										.getSingleResult()
										.thenAccept( count -> context.assertEquals(0l, count) )
										.thenAccept( vv -> s.close() )
								)
						)
		);
	}

	@Test
	public void testMetamodel(TestContext context) {
		EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity(GuineaPig.class);
		context.assertNotNull(pig);
		context.assertEquals( 3, pig.getAttributes().size() );
		context.assertEquals( "GuineaPig", pig.getName() );
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
	}

	@Entity(name="GuineaPig")
	@Table(name="Pig")
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

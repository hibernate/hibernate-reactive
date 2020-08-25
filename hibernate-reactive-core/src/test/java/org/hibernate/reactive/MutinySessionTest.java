/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;
import org.hibernate.LockMode;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.metamodel.EntityType;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;


public class MutinySessionTest extends BaseMutinyTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	private Uni<Integer> populateDB() {
		return getSessionFactory()
				.withSession(
						session -> session.persist( new GuineaPig(5, "Aloi") )
								.map( v -> { session.flush(); return null; } )
				);
	}

	private Uni<String> selectNameFromId(Integer id) {
		return getSessionFactory().withSession(
				session -> session.createQuery("SELECT name FROM GuineaPig WHERE id = " + id )
						.getResultList()
						.map(
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
	public void reactiveFind1(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.onItem().transformToUni( i -> openSession() )
						.onItem().invokeUni( session -> session.find( GuineaPig.class, expectedPig.getId() )
						.onItem().invoke( actualPig -> {
							assertThatPigsAreEqual( context, expectedPig, actualPig );
							context.assertTrue( session.contains( actualPig ) );
							context.assertFalse( session.contains( expectedPig ) );
							context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
							session.detach( actualPig );
							context.assertFalse( session.contains( actualPig ) );
						} )
						.onTermination().invoke( (v, err, c) -> session.close() )
				)

		);
	}

	@Test
	public void reactiveFind2(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.chain( i -> openSession() )
						.chain( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertTrue( session.contains( actualPig ) );
									context.assertFalse( session.contains( expectedPig ) );
									context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
									session.detach( actualPig );
									context.assertFalse( session.contains( actualPig ) );
								} )
								.eventually( () -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
								} )
								.eventually( () -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.chain( pig -> session.refresh(pig, LockMode.PESSIMISTIC_WRITE).map( v -> pig ) )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
								} )
								.eventually( () -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.chain( pig -> session.lock(pig, LockMode.PESSIMISTIC_READ).map( v -> pig ) )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
								} )
								.eventually( () -> session.close() )
						)
		);
	}

	@Test
	public void reactiveFindThenWriteLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.chain( pig -> session.lock(pig, LockMode.PESSIMISTIC_WRITE).map( v -> pig ) )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
								} )
								.eventually( () -> session.close() )
						)
		);
	}

	@Test
	public void reactivePersist1(TestContext context) {
		test(
				context,
				openSession()
						.onItem().invokeUni( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.onItem().invokeUni( s -> s.flush() )
						.onTermination().invoke( (s, e, c) -> s.close() )
						.onItem().invokeUni( v -> selectNameFromId( 10 ).onItem().invoke(
								selectRes -> context.assertEquals( "Tulip", selectRes ) ) )
		);
	}

	@Test
	public void reactivePersist2(TestContext context) {
		test(
				context,
				openSession()
						.chain( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.chain( s -> s.flush().eventually(s::close) )
						.chain( v -> selectNameFromId( 10 ) )
						.invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersistInTx(TestContext context) {
		test(
				context,
				openSession()
						.chain(
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
//												.thenCompose( vv -> s.flush() )
								)
						)
						.chain( vv -> selectNameFromId( 10 ) )
						.invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRollbackTx(TestContext context) {
		test(
				context,
				openSession()
						.chain(
								s -> s.withTransaction(
										t -> s.persist( new ReactiveSessionTest.GuineaPig( 10, "Tulip" ) )
												.chain( vv -> s.flush() )
												.map( vv -> {
													throw new RuntimeException();
												} )
								)
										.eventually( () -> s.close() )
						)
						.on().failure().recoverWithItem((Object)null)
						.chain( vv -> selectNameFromId( 10 ) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(TestContext context) {
		test(
				context,
				openSession()
						.chain(
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
												.chain( vv -> s.flush() )
												.invoke( vv -> t.markForRollback() )
								)
										.eventually( () -> s.close() )
						)
						.chain( vv -> selectNameFromId( 10 ) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().invokeUni( v -> selectNameFromId( 5 ).onItem().invoke( context::assertNotNull ) )
						.onItem().transformToUni( v -> openSession() )
						.onItem().invokeUni( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().invokeUni( session -> session.flush() )
						.onTermination().invoke( (session, err, c) -> session.close() )
						.onItem().invokeUni( v -> selectNameFromId( 5 ).onItem().invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.chain( v -> selectNameFromId( 5 ) )
						.invoke( context::assertNotNull )
						.chain( v -> openSession() )
						.chain( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.chain( session -> session.flush().eventually(session::close) )
						.chain( v -> selectNameFromId( 5 ) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().transformToUni( v -> openSession() )
						.onItem().invokeUni( session ->
								session.find( GuineaPig.class, 5 )
										.onItem().invokeUni( aloi -> session.remove( aloi ) )
										.onItem().invokeUni( v -> session.flush() )
										.onTermination().invoke( (v, e, c) -> session.close() )
										.onItem().invokeUni( v -> selectNameFromId( 5 ).onItem().invoke( context::assertNull ) ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session ->
							session.find( GuineaPig.class, 5 )
								.chain( aloi -> session.remove( aloi ) )
								.chain( v -> session.flush() )
								.eventually( () -> session.close() )
								.chain( v -> selectNameFromId( 5 ) )
								.invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().invokeUni( i ->
								getSessionFactory().withTransaction( (session, transaction) ->
										session.find( GuineaPig.class, 5 )
												.onItem().invokeUni( aloi -> session.remove( aloi ) )
								)
						)
						.onItem().invokeUni( v -> selectNameFromId( 5 ).onItem().invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx2(TestContext context) {
		test(
				context,
				populateDB()
						.chain( i ->
								getSessionFactory().withTransaction( (session, transaction) ->
										session.find( GuineaPig.class, 5 ).chain( session::remove )
								)
						)
						.chain( v -> selectNameFromId( 5 ) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session -> session.find( GuineaPig.class, 5 )
								.map( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
									return null;
								} )
								.chain( v -> session.flush() )
								.eventually( () -> session.close() )
						)
						.chain( v -> selectNameFromId( 5 ) )
						.invoke( name -> context.assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveUpdateVersion(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.chain( v -> openSession() )
						.chain( session -> session.find( GuineaPig.class, 5 )
								.map( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
									return null;
								} )
								.chain( v -> session.flush() )
								.eventually( () -> session.close() )
						)
						.chain( v -> selectNameFromId( 5 ) )
						.invoke( name -> context.assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveQueryWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.chain( v -> getSessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
										.setLockMode(LockMode.PESSIMISTIC_WRITE)
										.getSingleResult()
										.invoke( actualPig -> {
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
						.chain(
								v -> getSessionFactory().withTransaction(
										(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
												.setLockMode("pig", LockMode.PESSIMISTIC_WRITE )
												.getSingleResult()
												.invoke( actualPig -> {
													assertThatPigsAreEqual( context, expectedPig, actualPig );
													context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
												} )
								)
						)
		);
	}

	@Test
	public void reactiveMultiQuery(TestContext context) {
		GuineaPig foo = new GuineaPig( 5, "Foo" );
		GuineaPig bar = new GuineaPig( 6, "Bar" );
		GuineaPig baz = new GuineaPig( 7, "Baz" );
		AtomicInteger i = new AtomicInteger();

		test( context,
				getSessionFactory().withTransaction( (session, transaction) -> session.persist(foo, bar, baz) )
						.chain( v -> getSessionFactory().withSession(
								session -> session.createQuery("from GuineaPig", GuineaPig.class).getResults()
										.invoke( pig -> {
											context.assertNotNull(pig);
											i.getAndIncrement();
										} )
										.collectItems().asList()
										.invoke( list -> {
											context.assertEquals(3, i.get());
											context.assertEquals(3, list.size());
										} )
						) )
		);
	}

	@Test
	public void testMetamodel(TestContext context) {
		EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity(GuineaPig.class);
		context.assertNotNull(pig);
		context.assertEquals( 2, pig.getAttributes().size() );
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

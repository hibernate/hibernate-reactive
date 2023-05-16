/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.LockMode;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Table;
import jakarta.persistence.metamodel.EntityType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class MutinySessionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	private Uni<Void> populateDB() {
		return getMutinySessionFactory()
				.withSession( session -> session.persist( new GuineaPig( 5, "Aloi" ) )
						.chain( session::flush ) );
	}

	private Uni<String> selectNameFromId(Integer id) {
		return getMutinySessionFactory().withSession(
				session -> session.createQuery("SELECT name FROM GuineaPig WHERE id = " + id )
						.getResultList()
						.map( MutinySessionTest::nameFromResult )
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
	public void sessionClear(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 81, "Perry" );
		test(
				context,
				getMutinySessionFactory().withSession( session -> session
						.persist( guineaPig )
						.invoke( session::clear )
						// If the previous clear doesn't work, this will cause a duplicated entity exception
						.chain( () -> session.persist( guineaPig ) )
						.call( session::flush )
						.chain( () -> session.createQuery( "FROM GuineaPig", GuineaPig.class )
								// By not using .find() we check that there is only one entity in the db with getSingleResult()
								.getSingleResult() )
						.invoke( result -> assertThatPigsAreEqual( guineaPig, result ) )
				)
		);
	}

	@Test
	public void reactiveWithTransactionStatelessSession(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 61, "Mr. Peanutbutter" );
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insert( guineaPig ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( s -> s.find( GuineaPig.class, guineaPig.getId() ) ) )
				.invoke( result -> assertThatPigsAreEqual(  guineaPig, result ) )
		);
	}

	@Test
	public void reactiveWithTransactionSession(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 61, "Mr. Peanutbutter" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( guineaPig ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( s -> s.find( GuineaPig.class, guineaPig.getId() ) ) )
				.invoke( result -> assertThatPigsAreEqual(  guineaPig, result ) )
		);
	}

	@Test
	public void reactiveFind1(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.onItem().invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertTrue( session.contains( actualPig ) );
											assertFalse( session.contains( expectedPig ) );
											assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
											session.detach( actualPig );
											assertFalse( session.contains( actualPig ) );
										} )
						) )

		);
	}

	@Test
	public void reactiveFind2(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertTrue( session.contains( actualPig ) );
											assertFalse( session.contains( expectedPig ) );
											assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
											session.detach( actualPig );
											assertFalse( session.contains( actualPig ) );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.call( pig -> session.refresh(pig, LockMode.PESSIMISTIC_WRITE) )
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindReadOnlyRefreshWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
							session -> session.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> {
									session.setReadOnly(pig, true);
									pig.setName("XXXX");
									return session.flush()
											.call( v -> session.refresh(pig) )
											.invoke( v -> {
												assertEquals(expectedPig.name, pig.name);
												assertTrue(session.isReadOnly(pig));
											} );
								} )
						) )
						.call( () -> getMutinySessionFactory().withSession(
							session -> session.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> {
									session.setReadOnly(pig, false);
									pig.setName("XXXX");
									return session.flush()
											.call( v -> session.refresh(pig) )
											.invoke( v -> {
												assertEquals("XXXX", pig.name);
												assertFalse(session.isReadOnly(pig));
											} );
								} )
						) )
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.call( pig -> session.lock(pig, LockMode.PESSIMISTIC_READ) )
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindThenWriteLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.call( pig -> session.lock(pig, LockMode.PESSIMISTIC_WRITE) )
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactivePersist1(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withSession( s -> s.persist( new GuineaPig( 10, "Tulip" ) ).onItem().call( s::flush ) )
						.onItem().transformToUni( v -> selectNameFromId(10) )
						.onItem().invoke( selectRes -> assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersist2(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory().withSession( s -> s.persist( new GuineaPig( 10, "Tulip" ) ).chain( s::flush ) )
						.chain( () -> selectNameFromId(10) )
						.invoke( selectRes -> assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersistInTx(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (s,t) -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.chain( () -> selectNameFromId(10) )
						.invoke( selectRes -> assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRollbackTx(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction(
								(s,t) -> s.persist( new ReactiveSessionTest.GuineaPig( 10, "Tulip" ) )
										.call(s::flush)
										.invoke( () -> { throw new RuntimeException(); } )
						)
						.onItem().invoke( (Runnable) Assertions::fail )
						.onFailure().recoverWithItem((Void) null)
						.chain( () -> selectNameFromId(10) )
						.invoke( Assertions::assertNull )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction(
								(s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) )
										.call(s::flush)
										.invoke(t::markForRollback)
						)
						.chain( () -> selectNameFromId(10) )
						.invoke( Assertions::assertNull )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity1(VertxTestContext context) {
		test(
				context,
				populateDB()
						.onItem().call( () -> selectNameFromId(5).onItem().invoke( Assertions::assertNotNull ) )
						.chain( this::openMutinySession )
						.onItem().call( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().invoke( (Runnable) Assertions::fail )
						.onFailure().recoverWithItem( () -> null )
//						.onItem().invokeUni( session -> session.flush() )
//						.onTermination().invoke( (session, err, c) -> session.close() )
//						.onItem().invokeUni( v -> selectNameFromId( 5 ).onItem().invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity2(VertxTestContext context) {
		test(
				context,
				populateDB()
						.chain( () -> selectNameFromId(5) )
						.invoke( Assertions::assertNotNull )
						.chain( this::openMutinySession )
						.call( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().invoke( (Runnable) Assertions::fail )
						.onFailure().recoverWithItem( () -> null )
//						.chain( session -> session.flush().eventually(session::close) )
//						.then( () -> selectNameFromId( 5 ) )
//						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity1(VertxTestContext context) {
		test(
				context,
				populateDB()
						.onItem().call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.onItem().call(session::remove)
										.onItem().call(session::flush)
						) )
						.onItem().call( () -> selectNameFromId(5).onItem().invoke( Assertions::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity2(VertxTestContext context) {
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.chain(session::remove)
										.call(session::flush)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( Assertions::assertNull )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx1(VertxTestContext context) {
		test(
				context,
				populateDB()
						.onItem().call( () -> getMutinySessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, 5 )
										.onItem().call(session::remove)
						) )
						.onItem().call( () -> selectNameFromId(5).onItem().invoke( Assertions::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx2(VertxTestContext context) {
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, 5 )
										.call(session::remove)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( Assertions::assertNull )
		);
	}

	@Test
	public void reactiveUpdate(VertxTestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.map( pig -> {
											assertNotNull( pig );
											// Checking we are actually changing the name
											assertNotEquals( pig.getName(), NEW_NAME );
											pig.setName( NEW_NAME );
											return null;
										} )
										.call(session::flush)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( name -> assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveUpdateVersion(VertxTestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.map( pig -> {
											assertNotNull( pig );
											// Checking we are actually changing the name
											assertNotEquals( pig.getName(), NEW_NAME );
											pig.setName( NEW_NAME );
											return null;
										} )
										.call(session::flush)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( name -> assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveQueryWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
										.setLockMode( LockModeType.PESSIMISTIC_WRITE )
										.getSingleResult()
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveQueryWithAliasedLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
										.setLockMode("pig", LockMode.PESSIMISTIC_WRITE )
										.getSingleResult()
										.invoke( actualPig -> {
											assertThatPigsAreEqual(  expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveMultiQuery(VertxTestContext context) {
		GuineaPig foo = new GuineaPig( 5, "Foo" );
		GuineaPig bar = new GuineaPig( 6, "Bar" );
		GuineaPig baz = new GuineaPig( 7, "Baz" );
		AtomicInteger i = new AtomicInteger();

		test( context,
				getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session.persistAll(foo, bar, baz) )
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.createQuery("from GuineaPig", GuineaPig.class)
										.getResultList().onItem().disjoint()
										.invoke( pig -> {
											assertNotNull(pig);
											i.getAndIncrement();
										} )
										.collect().asList()
										.invoke( list -> {
											assertEquals(3, i.get());
											assertEquals(3, list.size());
										} )
						) )
		);
	}

	@Test
	public void reactiveClose(VertxTestContext context) {
		test( context, openMutinySession()
				.invoke( session -> assertTrue( session.isOpen() ) )
				.call( Mutiny.Session::close )
				.invoke( session -> assertFalse( session.isOpen() ) )
		);
	}

	@Test
	public void testMetamodel() {
		EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity(GuineaPig.class);
		assertNotNull(pig);
		assertEquals( 2, pig.getAttributes().size() );
		assertEquals( "GuineaPig", pig.getName() );
	}

	@Test
	public void testTransactionPropagation(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction(
				(session, transaction) -> session.createQuery("from GuineaPig").getResultList()
						.chain( list -> {
							assertNotNull( session.currentTransaction() );
							assertFalse( session.currentTransaction().isMarkedForRollback() );
							session.currentTransaction().markForRollback();
							assertTrue( session.currentTransaction().isMarkedForRollback() );
							assertTrue( transaction.isMarkedForRollback() );
							return session.withTransaction( t -> {
								assertTrue( t.isMarkedForRollback() );
								return session.createQuery("from GuineaPig").getResultList();
							} );
						} )
		) );
	}

	@Test
	public void testSessionPropagation(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> {
			assertFalse( session.isDefaultReadOnly() );
			session.setDefaultReadOnly(true);
			return session.createQuery("from GuineaPig").getResultList()
					.chain( list -> getMutinySessionFactory().withSession(s -> {
						assertTrue( s.isDefaultReadOnly() );
						return s.createQuery("from GuineaPig").getResultList();
					} ) );
		} ) );
	}

	@Test
	public void testDupeException(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction((s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) ))
				.chain(() -> getMutinySessionFactory()
						.withTransaction((s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) ))
				).onItemOrFailure().invoke((i, t) -> {
					assertNotNull(t);
					assertTrue(t instanceof PersistenceException);
				})
				.onFailure().recoverWithNull()
		);
	}

	@Test
	public void testExceptionInWithSession(VertxTestContext context) {
		final Mutiny.Session[] savedSession = new Mutiny.Session[1];
		test( context, getMutinySessionFactory()
				.withSession( session -> {
					assertTrue( session.isOpen() );
					savedSession[0] = session;
					throw new RuntimeException( "No Panic: This is just a test" );
				} )
				.onItem().invoke( () -> fail( "Test should throw an exception" ) )
				.onFailure()
				.recoverWithNull()
				.invoke( () -> assertFalse( savedSession[0].isOpen(), "Session should be closed" ) )
		);
	}

	@Test
	public void testExceptionInWithTransaction(VertxTestContext context) {
		final Mutiny.Session[] savedSession = new Mutiny.Session[1];
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> {
					assertTrue( session.isOpen() );
					savedSession[0] = session;
					throw new RuntimeException( "No Panic: This is just a test" );
				} )
				.onItem().invoke( () -> fail( "Test should throw an exception" ) )
				.onFailure()
				.recoverWithNull()
				.invoke( () -> assertFalse( savedSession[0].isOpen(), "Session should be closed" ) )
		);
	}

	@Test
	public void testExceptionInWithStatelessSession(VertxTestContext context) {
		final Mutiny.StatelessSession[] savedSession = new Mutiny.StatelessSession[1];
		test( context, getMutinySessionFactory()
				.withStatelessSession( session -> {
					assertTrue( session.isOpen() );
					savedSession[0] = session;
					throw new RuntimeException( "No Panic: This is just a test" );
				} )
				.onItem().invoke( () -> fail( "Test should throw an exception" ) )
				.onFailure()
				.recoverWithNull()
				.invoke( () -> assertFalse( savedSession[0].isOpen(), "Session should be closed" ) )
		);
	}

	@Test
	public void testForceFlushWithDelete(VertxTestContext context) {
		// Pig1 and Pig2 must have the same id
		final GuineaPig pig1 = new GuineaPig( 111, "Pig 1" );
		final GuineaPig pig2 = new GuineaPig( 111, "Pig 2" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.persist( pig1 )
						.call( () -> session.remove( pig1 ) )
						// pig 2 has the same id as pig1.
						// If pig1 has not been removed from the session,
						// we will have a duplicated id exception
						.call( () -> session.persist( pig2 ) )
				)
				.chain( () -> getMutinySessionFactory()
						.withSession( s -> s.find( GuineaPig.class, pig2.getId() ) ) )
				.invoke( result -> assertThatPigsAreEqual( pig2, result ) )
		);
	}

	private void assertThatPigsAreEqual(GuineaPig expected, GuineaPig actual) {
		assertNotNull( actual );
		assertEquals( expected.getId(), actual.getId() );
		assertEquals( expected.getName(), actual.getName() );
	}

	@Entity(name = "GuineaPig")
	@Table(name = "Pig")
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

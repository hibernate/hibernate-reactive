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
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.metamodel.EntityType;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


public class MutinySessionTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	private Uni<Void> populateDB() {
		return getMutinySessionFactory()
				.withSession(
						session -> session.persist( new GuineaPig(5, "Aloi") )
								.invoke(session::flush)
				);
	}

	private Uni<String> selectNameFromId(Integer id) {
		return getMutinySessionFactory().withSession(
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
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.onItem().invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertTrue( session.contains( actualPig ) );
											context.assertFalse( session.contains( expectedPig ) );
											context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
											session.detach( actualPig );
											context.assertFalse( session.contains( actualPig ) );
										} )
						) )

		);
	}

	@Test
	public void reactiveFind2(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertTrue( session.contains( actualPig ) );
											context.assertFalse( session.contains( expectedPig ) );
											context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
											session.detach( actualPig );
											context.assertFalse( session.contains( actualPig ) );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.call( pig -> session.refresh(pig, LockMode.PESSIMISTIC_WRITE) )
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.call( pig -> session.lock(pig, LockMode.PESSIMISTIC_READ) )
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
										} )
						) )
		);
	}

	@Test
	public void reactiveFindThenWriteLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, expectedPig.getId() )
										.call( pig -> session.lock(pig, LockMode.PESSIMISTIC_WRITE) )
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactivePersist1(TestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withSession( s -> s.persist( new GuineaPig( 10, "Tulip" ) ).onItem().call(s::flush) )
						.onItem().transformToUni( v -> selectNameFromId(10) )
						.onItem().invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )

		);
	}

	@Test
	public void reactivePersist2(TestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withSession( s -> s.persist( new GuineaPig( 10, "Tulip" ) ).chain(s::flush) )
						.chain( () -> selectNameFromId(10) )
						.invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersistInTx(TestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (s,t) -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.chain( () -> selectNameFromId(10) )
						.invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRollbackTx(TestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction(
								(s,t) -> s.persist( new ReactiveSessionTest.GuineaPig( 10, "Tulip" ) )
										.call(s::flush)
										.invoke( () -> { throw new RuntimeException(); } )
						)
						.onItem().invoke( () -> context.fail() )
						.onFailure().recoverWithItem((Void) null)
						.chain( () -> selectNameFromId(10) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(TestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction(
								(s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) )
										.call(s::flush)
										.invoke(t::markForRollback)
						)
						.chain( () -> selectNameFromId(10) )
						.invoke( context::assertNull )
		);
	}

	/**
	 * Counterpart to {@link #reactiveProxyConnectionRaceInner(TestContext)},
	 * the {@code Mutiny.Session.find()} operation is "created" <em>before</em>
	 * {@code Mutiny.Session.withTransaction()} runs.
	 * <p>
	 * See {@link org.hibernate.reactive.pool.impl.ProxyConnectionRaceTest} and
	 * <a href="https://github.com/hibernate/hibernate-reactive/issues/475">#475</a>.
	 * </p>
	 * <p>
	 * See javadoc of {@link Uni}: <em>"{@link Uni It} follows the subscription
	 * pattern, meaning that the action is only triggered once a
	 * {@link io.smallrye.mutiny.subscription.UniSubscriber UniSubscriber}
	 * subscribes to the {@link Uni}."</em> This probably means that
	 * borrowing/creating a connection in the {@code Mutiny.Session.find},
	 * {@code Mutiny.Session.withTransaction}, {@code Mutiny.Query.getSingleResult},
	 * etc functions should only happen when a
	 * {@link io.smallrye.mutiny.subscription.UniSubscriber UniSubscriber}
	 * subscribes to the "outermost" {@link Uni} (the
	 * {@code Mutiny.Session.withTransaction} in this test case).
	 * </p>
	 */
	@Test
	public void reactiveProxyConnectionRace(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().transform( v -> openMutinySession() )
						.onItem().transform( session -> {
							CountDownLatch synchronizer = new CountDownLatch(1);
							try {
								Mutiny.Session finalSession = injectDelayedConnection(session, synchronizer);

								// The race is triggered in the withTransaction(),
								// when the connection needed by this find() could
								// not be acquired until withTransaction() gets called.

								Uni<GuineaPig> findWork = finalSession.find(GuineaPig.class, 3);

								// still no connection acquired
								return finalSession.withTransaction(tx -> findWork);
							}
							finally {
								// connection acquired when the latch counts down
								synchronizer.countDown();
							}
						}
				)
		);
	}

	/**
	 * Counterpart to {@link #reactiveProxyConnectionRace(TestContext)}, but the
	 * {@code Mutiny.Session.find()} operation is "created" <em>after</em>
	 * {@code Mutiny.Session.withTransaction()} ran, so this test case does not
	 * trigger the race condition.
	 * <p>
	 * See {@link org.hibernate.reactive.pool.impl.ProxyConnectionRaceTest} and
	 * <a href="https://github.com/hibernate/hibernate-reactive/issues/475">#475</a>.
	 * </p>
	 */
	@Test
	public void reactiveProxyConnectionRaceInner(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().transform( v -> openMutinySession() )
						.onItem().transform( session -> {
							CountDownLatch synchronizer = new CountDownLatch(1);
							try {
								Mutiny.Session finalSession = injectDelayedConnection(session, synchronizer);
								return finalSession.withTransaction(tx ->
									finalSession.find(GuineaPig.class, 3));
							}
							finally {
								// connection acquired when the latch counts down
								synchronizer.countDown();
							}
						})
		);
	}

	@Test
	public void reactiveRemoveTransientEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().call( () -> selectNameFromId(5).onItem().invoke( context::assertNotNull ) )
						.onItem().transform( v -> openMutinySession() )
						.onItem().call( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().invoke( () -> context.fail() )
						.onFailure().recoverWithItem( () -> null )
//						.onItem().invokeUni( session -> session.flush() )
//						.onTermination().invoke( (session, err, c) -> session.close() )
//						.onItem().invokeUni( v -> selectNameFromId( 5 ).onItem().invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.chain( () -> selectNameFromId(5) )
						.invoke( context::assertNotNull )
						.replaceWith( this::openMutinySession )
						.call( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().invoke( () -> context.fail() )
						.onFailure().recoverWithItem( () -> null )
//						.chain( session -> session.flush().eventually(session::close) )
//						.then( () -> selectNameFromId( 5 ) )
//						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.onItem().call(session::remove)
										.onItem().call(session::flush)
						) )
						.onItem().call( () -> selectNameFromId(5).onItem().invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.chain(session::remove)
										.call(session::flush)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().call( () -> getMutinySessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, 5 )
										.onItem().call(session::remove)
						) )
						.onItem().call( () -> selectNameFromId(5).onItem().invoke( context::assertNull ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx2(TestContext context) {
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction(
								(session, transaction) -> session.find( GuineaPig.class, 5 )
										.call(session::remove)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( context::assertNull )
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.map( pig -> {
											context.assertNotNull( pig );
											// Checking we are actually changing the name
											context.assertNotEquals( pig.getName(), NEW_NAME );
											pig.setName( NEW_NAME );
											return null;
										} )
										.call(session::flush)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( name -> context.assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveUpdateVersion(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withSession(
								session -> session.find( GuineaPig.class, 5 )
										.map( pig -> {
											context.assertNotNull( pig );
											// Checking we are actually changing the name
											context.assertNotEquals( pig.getName(), NEW_NAME );
											pig.setName( NEW_NAME );
											return null;
										} )
										.call(session::flush)
						) )
						.chain( () -> selectNameFromId(5) )
						.invoke( name -> context.assertEquals( NEW_NAME, name ) )
		);
	}

	@Test
	public void reactiveQueryWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
										.setLockMode(LockMode.PESSIMISTIC_WRITE)
										.getSingleResult()
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveQueryWithAliasedLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class)
										.setLockMode("pig", LockMode.PESSIMISTIC_WRITE )
										.getSingleResult()
										.invoke( actualPig -> {
											assertThatPigsAreEqual( context, expectedPig, actualPig );
											context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
										} )
						) )
		);
	}

	@Test
	public void reactiveMultiQuery(TestContext context) {
		GuineaPig foo = new GuineaPig( 5, "Foo" );
		GuineaPig bar = new GuineaPig( 6, "Bar" );
		GuineaPig baz = new GuineaPig( 7, "Baz" );
		AtomicInteger i = new AtomicInteger();

		test( context,
				getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session.persistAll(foo, bar, baz) )
						.call( () -> getMutinySessionFactory().withSession(
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

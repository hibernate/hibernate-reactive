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

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.EntityType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 10, timeUnit = MINUTES)
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
				session -> session
						.createSelectionQuery( "SELECT name FROM GuineaPig WHERE id = " + id, String.class )
						.getResultList()
						.map( MutinySessionTest::nameFromResult )
		);
	}

	private static String nameFromResult(List<String> rowSet) {
		return switch ( rowSet.size() ) {
			case 0 -> null;
			case 1 -> rowSet.get( 0 );
			default -> throw new AssertionError( "More than one result returned: " + rowSet.size() );
		};
	}

	@Test
	public void reactiveFindMultipleIds(VertxTestContext context) {
		final GuineaPig rump = new GuineaPig( 55, "Rumpelstiltskin" );
		final GuineaPig emma = new GuineaPig( 77, "Emma" );
		test(
				context, populateDB()
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s
								.persistAll( emma, rump ) )
						)
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s
								.find( GuineaPig.class, emma.getId(), rump.getId() )
						) )
						.invoke( pigs -> assertThat( pigs ).containsExactlyInAnyOrder( emma, rump ) )
		);
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
						.chain( () -> session.createSelectionQuery( "FROM GuineaPig", GuineaPig.class )
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
	public void reactiveFind(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.contains( actualPig ) ).isTrue();
									assertThat( session.contains( expectedPig ) ).isFalse();
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.READ );
									session.detach( actualPig );
									assertThat( session.contains( actualPig ) ).isFalse();
								} )
						) )
		);
	}

	@Test
	public void reactiveFindWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
								} )
						) )
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> session.refresh( pig, LockMode.PESSIMISTIC_WRITE ) )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
								} )
						) )
		);
	}

	@Test
	public void reactiveFindReadOnlyRefreshWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> {
									session.setReadOnly( pig, true );
									pig.setName( "XXXX" );
									return session.flush()
											.call( v -> session.refresh( pig ) )
											.invoke( v -> {
												assertThat( pig.name ).isEqualTo( expectedPig.name );
												assertThat( session.isReadOnly( pig ) ).isTrue();
											} );
								} )
						) )
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> {
									session.setReadOnly( pig, false );
									pig.setName( "XXXX" );
									return session.flush()
											.call( v -> session.refresh( pig ) )
											.invoke( v -> {
												assertThat( pig.name ).isEqualTo( "XXXX" );
												assertThat( session.isReadOnly( pig ) ).isFalse();
											} );
								} )
						) )
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> session.lock( pig, LockMode.PESSIMISTIC_READ ) )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_READ );
								} )
						) )
		);
	}

	@Test
	public void reactiveFindThenWriteLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.call( pig -> session.lock( pig, LockMode.PESSIMISTIC_WRITE ) )
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
								} )
						) )
		);
	}

	@Test
	public void reactivePersist(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withSession( s -> s.persist( new GuineaPig( 10, "Tulip" ) ).call( s::flush ) )
						.chain( () -> selectNameFromId( 10 ) )
						.invoke( selectRes -> assertThat( selectRes ).isEqualTo( "Tulip" ) )
		);
	}

	@Test
	public void reactivePersistInTx(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.chain( () -> selectNameFromId( 10 ) )
						.invoke( selectRes -> assertThat( selectRes ).isEqualTo( "Tulip" ) )
		);
	}

	@Test
	public void reactiveRollbackTx(VertxTestContext context) {
		final RuntimeException expectedException = new RuntimeException( "For test, After flush" );
		test(
				context, assertThrown(
						RuntimeException.class, getMutinySessionFactory()
								.withTransaction( s -> s
										.persist( new GuineaPig( 10, "Tulip" ) )
										// Flush the changes but don't commit the transaction
										.call( s::flush )
										.invoke( () -> {
											// Throw an exception before committing the transaction
											throw expectedException;
										} )
								)
				)
						.invoke( e -> assertThat( e ).hasMessage( expectedException.getMessage() ) )
						.chain( () -> selectNameFromId( 10 ) )
						.invoke( name -> assertThat( name ).isNull() )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( (s, t) -> s
								.persist( new GuineaPig( 10, "Tulip" ) )
								.call( s::flush )
								.invoke( t::markForRollback )
						)
						.chain( () -> selectNameFromId( 10 ) )
						.invoke( name -> assertThat( name ).isNull() )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity(VertxTestContext context) {
		test(
				context, populateDB()
						.chain( () -> selectNameFromId( 5 ) )
						.invoke( name -> assertThat( name ).isNotNull() )
						.chain( this::openMutinySession )
						.chain( session -> assertThrown(
								HibernateException.class,
								session.remove( new GuineaPig( 5, "Aloi" ) )
						) )
						.invoke( e -> assertThat( e )
								.hasMessageContaining( "unmanaged instance passed to remove" )
						)
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(VertxTestContext context) {
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withTransaction( session -> session
								.find( GuineaPig.class, 5 )
								.call( session::remove )
						) )
						.chain( () -> selectNameFromId( 5 ) )
						.invoke( name -> assertThat( name ).isNull() )
		);
	}

	@Test
	public void reactiveRemoveManagedEntityWithTx(VertxTestContext context) {
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withTransaction( session -> session
								.find( GuineaPig.class, 5 )
								.call( session::remove )
						) )
						.chain( () -> selectNameFromId( 5 ) )
						.invoke( name -> assertThat( name ).isNull() )
		);
	}

	@Test
	public void reactiveUpdate(VertxTestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, 5 )
								.invoke( pig -> {
									assertThat( pig ).isNotNull();
									// Checking we are actually changing the name
									assertThat( pig.getName() ).isNotEqualTo( NEW_NAME );
									pig.setName( NEW_NAME );
								} )
								.call( session::flush )
						) )
						.chain( () -> selectNameFromId( 5 ) )
						.invoke( name -> assertThat( name ).isEqualTo( NEW_NAME ) )
		);
	}

	@Test
	public void reactiveUpdateVersion(VertxTestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context, populateDB()
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.find( GuineaPig.class, 5 )
								.invoke( pig -> {
									assertThat( pig ).isNotNull();
									// Checking we are actually changing the name
									assertThat( pig.getName() ).isNotEqualTo( NEW_NAME );
									assertThat( pig.version ).isEqualTo( 0 );
									pig.setName( NEW_NAME );
									pig.version = 10; //ignored by Hibernate
								} )
								.call( session::flush ) )
						)
						.chain( () -> getMutinySessionFactory()
								.withSession( s -> s.find( GuineaPig.class, 5 ) ) )
						.invoke( pig -> assertThat( pig.version ).isEqualTo( 1 ) )
		);
	}

	@Test
	public void reactiveQueryWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB().call( () -> getMutinySessionFactory()
						.withTransaction( session -> session
								.createSelectionQuery( "from GuineaPig pig", GuineaPig.class )
								.setLockMode( LockModeType.PESSIMISTIC_WRITE )
								.getSingleResult()
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
								} )
						) )
		);
	}

	@Test
	public void reactiveQueryWithAliasedLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB().call( () -> getMutinySessionFactory()
						.withTransaction( session -> session
								.createSelectionQuery( "from GuineaPig pig", GuineaPig.class )
								.setLockMode( "pig", LockMode.PESSIMISTIC_WRITE )
								.getSingleResult()
								.invoke( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
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

		test(
				context, getMutinySessionFactory()
						.withTransaction( session -> session.persistAll( foo, bar, baz ) )
						.call( () -> getMutinySessionFactory().withSession( session -> session
								.createSelectionQuery( "from GuineaPig", GuineaPig.class )
								.getResultList().onItem().disjoint()
								.invoke( pig -> {
									assertThat( pig ).isNotNull();
									i.getAndIncrement();
								} )
								.collect().asList()
								.invoke( list -> {
									assertThat( i.get() ).isEqualTo( 3 );
									assertThat( list.size() ).isEqualTo( 3 );
								} )
						) )
		);
	}

	@Test
	public void reactiveClose(VertxTestContext context) {
		test(
				context, openMutinySession()
						.invoke( session -> assertThat( session.isOpen() ).isTrue() )
						.call( Mutiny.Session::close )
						.invoke( session -> assertThat( session.isOpen() ).isFalse() )
		);
	}

	@Test
	void testFactory(VertxTestContext context) {
		test(
				context, getMutinySessionFactory().withSession( session -> {
					session.getFactory().getCache().evictAll();
					session.getFactory().getMetamodel().entity( GuineaPig.class );
					session.getFactory().getCriteriaBuilder().createQuery( GuineaPig.class );
					session.getFactory().getStatistics().isStatisticsEnabled();
					return Uni.createFrom().voidItem();
				} )
		);
	}

	@Test
	public void testMetamodel() {
		EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity( GuineaPig.class );
		assertThat( pig ).isNotNull();
		assertThat( pig.getAttributes() )
				.map( Attribute::getName )
				.containsExactlyInAnyOrder( "id", "version", "name" );
		assertThat( pig.getName() ).isEqualTo( "GuineaPig" );
	}

	@Test
	public void testTransactionPropagation(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.createSelectionQuery( "from GuineaPig", GuineaPig.class )
								.getResultList()
								.chain( list -> {
									assertThat( session.currentTransaction() ).isNotNull();
									assertThat( session.currentTransaction().isMarkedForRollback() ).isFalse();
									session.currentTransaction().markForRollback();
									assertThat( session.currentTransaction().isMarkedForRollback() ).isTrue();
									assertThat( session.currentTransaction().isMarkedForRollback() ).isTrue();
									assertThat( transaction.isMarkedForRollback() ).isTrue();
									return session.withTransaction( t -> {
										assertThat( t.isMarkedForRollback() ).isTrue();
										return session
												.createSelectionQuery( "from GuineaPig", GuineaPig.class )
												.getResultList();
									} );
								} )
						)
		);
	}

	@Test
	public void testSessionPropagation(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withSession( session -> {
							assertThat( session.isDefaultReadOnly() ).isFalse();
							session.setDefaultReadOnly( true );
							return session
									.createSelectionQuery( "from GuineaPig", GuineaPig.class )
									.getResultList()
									.chain( list -> getMutinySessionFactory().withSession( s -> {
										assertThat( s.isDefaultReadOnly() ).isTrue();
										return s.createSelectionQuery( "from GuineaPig", GuineaPig.class )
												.getResultList();
									} ) );
						} )
		);
	}

	@Test
	public void testDupeException(VertxTestContext context) {
		test(
				// It would make sense to check the error message, but it changes based on the database selected.
				// I think this is good enough for now.
				context, assertThrown(
						ConstraintViolationException.class, getMutinySessionFactory()
								.withTransaction( s -> s
										.persist( new GuineaPig( 10, "Tulip" ) )
								)
								.chain( () -> getMutinySessionFactory().withTransaction( s -> s
										.persist( new GuineaPig( 10, "Tulip" ) ) )
								)
				)
		);
	}

	@Test
	public void testExceptionInWithSession(VertxTestContext context) {
		final Mutiny.Session[] savedSession = new Mutiny.Session[1];
		test(
				context, assertThrown(
						RuntimeException.class, getMutinySessionFactory()
								.withSession( session -> {
									assertThat( session.isOpen() ).isTrue();
									savedSession[0] = session;
									throw new RuntimeException( "No Panic: This is just a test" );
								} )
				)
						.invoke( e -> assertThat( e ).hasMessageContaining( "No Panic:" ) )
						.invoke( () -> assertThat( savedSession[0].isOpen() )
								.as( "Session should be closed" )
								.isFalse() )
		);
	}

	@Test
	public void testExceptionInWithTransaction(VertxTestContext context) {
		final Mutiny.Session[] savedSession = new Mutiny.Session[1];
		test(
				context, assertThrown(
						RuntimeException.class, getMutinySessionFactory()
								.withTransaction( (session, tx) -> {
									assertThat( session.isOpen() ).isTrue();
									savedSession[0] = session;
									throw new RuntimeException( "No Panic: This is just a test" );
								} )
				)
						.invoke( e -> assertThat( e ).hasMessageContaining( "No Panic:" ) )
						.invoke( () -> assertThat( savedSession[0].isOpen() )
								.as( "Session should be closed" )
								.isFalse() )
		);
	}

	@Test
	public void testExceptionInWithStatelessSession(VertxTestContext context) {
		final Mutiny.StatelessSession[] savedSession = new Mutiny.StatelessSession[1];
		test(
				context, assertThrown(
						RuntimeException.class, getMutinySessionFactory()
								.withStatelessSession( session -> {
									assertThat( session.isOpen() ).isTrue();
									savedSession[0] = session;
									throw new RuntimeException( "No Panic: This is just a test" );
								} )
				)
						.invoke( e -> assertThat( e ).hasMessageContaining( "No Panic:" ) )
						.invoke( () -> assertThat( savedSession[0].isOpen() )
								.as( "Session should be closed" )
								.isFalse() )
		);
	}

	@Test
	public void testForceFlushWithDelete(VertxTestContext context) {
		// Pig1 and Pig2 must have the same id
		final GuineaPig pig1 = new GuineaPig( 111, "Pig 1" );
		final GuineaPig pig2 = new GuineaPig( 111, "Pig 2" );
		test(
				context, getMutinySessionFactory()
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

	@Test
	public void testCurrentSession(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withSession( session -> getMutinySessionFactory()
								.withSession( s -> {
									assertThat( s ).isEqualTo( session );
									Mutiny.Session currentSession = getMutinySessionFactory().getCurrentSession();
									assertThat( currentSession ).isNotNull();
									assertThat( currentSession.isOpen() ).isTrue();
									assertThat( currentSession ).isEqualTo( session );
									return Uni.createFrom().voidItem();
								} )
								.invoke( () -> assertThat( getMutinySessionFactory().getCurrentSession() ).isNotNull() )
						)
						.invoke( () -> assertThat( getMutinySessionFactory().getCurrentSession() ).isNull() )
		);
	}

	@Test
	public void testCurrentStatelessSession(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withStatelessSession( session -> getMutinySessionFactory()
								.withStatelessSession( s -> {
									assertEquals( session, s );
									Mutiny.StatelessSession currentSession = getMutinySessionFactory().getCurrentStatelessSession();
									assertThat( currentSession ).isNotNull();
									assertThat( currentSession.isOpen() ).isTrue();
									assertThat( currentSession ).isEqualTo( session );
									return Uni.createFrom().voidItem();
								} )
								.invoke( () -> assertThat( getMutinySessionFactory().getCurrentStatelessSession() ).isNotNull() )
						)
						.invoke( () -> assertThat( getMutinySessionFactory().getCurrentStatelessSession() ).isNull() )
		);
	}

	private void assertThatPigsAreEqual(GuineaPig expected, GuineaPig actual) {
		assertThat( actual ).isNotNull();
		assertThat( actual.getId() ).isEqualTo( expected.getId() );
		assertThat( actual.getName() ).isEqualTo( expected.getName() );
	}

	@Entity(name = "GuineaPig")
	@Table(name = "Pig")
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

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import jakarta.persistence.metamodel.EntityType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

@Timeout(value = 10, timeUnit = MINUTES)
public class ReactiveSessionTest extends BaseReactiveTest {

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( GuineaPig.class );
	}

	private CompletionStage<Void> populateDB() {
		return getSessionFactory()
				.withTransaction( s -> s.persist( new GuineaPig( 5, "Aloi" ) ) );
	}

	private CompletionStage<String> selectNameFromId(Integer id) {
		return openSession()
				.thenCompose( session -> session
						.find( GuineaPig.class, id )
						.thenCompose( pig -> session.close()
								.thenApply( v -> pig == null ? null : pig.getName() ) )
				);
	}

	@Test
	public void reactiveFind(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.contains( actualPig ) ).isTrue();
									assertThat( session.contains( expectedPig ) ).isFalse();
									assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.READ );
									session.detach( actualPig );
									assertThat( session.contains( actualPig ) ).isFalse();
								} )
						)
		);
	}

	@Test
	public void reactiveFindMultipleIds(VertxTestContext context) {
		final GuineaPig rump = new GuineaPig( 55, "Rumpelstiltskin" );
		final GuineaPig emma = new GuineaPig( 77, "Emma" );
		test(
				context, populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction( s -> s.persist( emma, rump ) ) )
						.thenCompose( v -> getSessionFactory().withTransaction( s -> s
								.find( GuineaPig.class, emma.getId(), rump.getId() ) )
						)
						.thenAccept( pigs -> assertThat( pigs ).containsExactlyInAnyOrder( emma, rump ) )
		);
	}

	@Test
	public void sessionClear(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 81, "Perry" );
		test(
				context,
				getSessionFactory().withSession( session -> session
						.persist( guineaPig )
						.thenAccept( v -> session.clear() )
						// If the previous clear doesn't work, this will cause a duplicated entity exception
						.thenCompose( v -> session.persist( guineaPig ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> session.createSelectionQuery( "FROM GuineaPig", GuineaPig.class )
								// By not using .find() we check that there is only one entity in the db with getSingleResult()
								.getSingleResult() )
						.thenAccept( result -> assertThatPigsAreEqual( guineaPig, result ) )
				)
		);
	}

	@Test
	public void reactiveWithTransactionSession(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 61, "Mr. Peanutbutter" );
		test(
				context, getSessionFactory()
						.withTransaction( session -> session.persist( guineaPig ) )
						.thenCompose( v -> getSessionFactory()
								.withSession( session -> session.find( GuineaPig.class, guineaPig.getId() ) ) )
						.thenAccept( result -> assertThatPigsAreEqual( guineaPig, result ) )
		);
	}

	@Test
	public void reactiveWithTransactionStatelessSession(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 61, "Mr. Peanutbutter" );
		test(
				context, getSessionFactory()
						.withStatelessTransaction( session -> session.insert( guineaPig ) )
						.thenCompose( v -> getSessionFactory()
								.withSession( session -> session.find( GuineaPig.class, guineaPig.getId() ) ) )
						.thenAccept( result -> assertThatPigsAreEqual( guineaPig, result ) )
		);
	}

	@Test
	public void reactivePersistFindDelete(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				openSession().thenCompose( session -> session
						.persist( guineaPig )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.detach( guineaPig ) )
						.thenAccept( v -> assertThat( session.contains( guineaPig ) ).isFalse() )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( guineaPig, actualPig );
							assertThat( session.contains( actualPig ) ).isTrue();
							assertThat( session.contains( guineaPig ) ).isFalse();
							assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.READ );
							session.detach( actualPig );
							assertThat( session.contains( actualPig ) ).isFalse();
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
		);
	}

	@Test
	public void reactiveFindWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );

		test(
				context, populateDB().thenCompose( v -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
								.thenAccept( actualPig -> {
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
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, tx) -> session
										.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( pig -> session
												.refresh( pig, LockMode.PESSIMISTIC_WRITE )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( expectedPig, pig );
													assertThat( session.getLockMode( pig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
												} )
										)
								) )
		);
	}

	@Test
	public void reactiveFindReadOnlyRefreshWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> {
									session.setReadOnly( pig, true );
									pig.setName( "XXXX" );
									return session.flush()
											.thenCompose( v -> session.refresh( pig ) )
											.thenAccept( v -> {
												assertThat( expectedPig.getName() ).isEqualTo( pig.getName() );
												assertThat( session.isReadOnly(  pig ) ).isTrue();
											} );
								} )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> {
									session.setReadOnly( pig, false );
									pig.setName( "XXXX" );
									return session.flush()
											.thenCompose( v -> session.refresh( pig ) )
											.thenAccept( v -> {
												assertThat( "XXXX" ).isEqualTo( pig.getName() );
												assertThat(  session.isReadOnly(  pig ) ).isFalse();
											} );
								} )
						)
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.thenCompose( unused -> getSessionFactory()
								.withTransaction( (session, tx) -> session
										.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( pig -> session
												.lock( pig, LockMode.PESSIMISTIC_READ )
												.thenAccept( v -> {
													assertThatPigsAreEqual( expectedPig, pig );
													assertThat( session.getLockMode( pig ) ).isEqualTo(  LockMode.PESSIMISTIC_READ );
												} )
										)
								)
						)
		);
	}

	@Test
	public void reactiveFindThenWriteLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB().thenCompose( v -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session
										.lock( pig, LockMode.PESSIMISTIC_WRITE )
										.thenAccept( vv -> {
											assertThatPigsAreEqual( expectedPig, pig );
											assertThat( session.getLockMode( pig ) ).isEqualTo(  LockMode.PESSIMISTIC_WRITE );
											assertThat( pig.version ).isEqualTo( 0 );
										} )
								)
						) )
		);
	}

	@Test
	public void reactiveFindThenForceLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock( pig, LockMode.PESSIMISTIC_FORCE_INCREMENT )
										.thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo(  LockMode.PESSIMISTIC_FORCE_INCREMENT );
									assertThat( actualPig.version ).isEqualTo( 1 );
								} )
								.thenCompose( v -> session.createSelectionQuery( "select version from GuineaPig", Integer.class )
										.getSingleResult() )
								.thenAccept( version -> assertThat( version ).isEqualTo( 1 ) )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock( pig, LockMode.PESSIMISTIC_FORCE_INCREMENT )
										.thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertThat( session.getLockMode( actualPig ) ).isEqualTo(  LockMode.PESSIMISTIC_FORCE_INCREMENT );
									assertThat( actualPig.version ).isEqualTo( 2 );
								} )
								.thenCompose( v -> session
										.createSelectionQuery( "select version from GuineaPig", Integer.class )
										.getSingleResult() )
								.thenAccept( version -> assertThat( version ).isEqualTo( 2 ) )
						)
		);
	}

	@Test
	public void reactiveFindWithPessimisticIncrementLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction(  session -> session.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_FORCE_INCREMENT )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo(  LockMode.PESSIMISTIC_FORCE_INCREMENT );
											assertThat( actualPig.version ).isEqualTo( 1 );
										} ) )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 1 ) )
		);
	}

	@Test
	public void reactiveFindWithOptimisticIncrementLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
									  .find( GuineaPig.class, expectedPig.getId(), LockMode.OPTIMISTIC_FORCE_INCREMENT )
									  .thenAccept( actualPig -> {
										  assertThatPigsAreEqual( expectedPig, actualPig );
										  assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.OPTIMISTIC_FORCE_INCREMENT );
										  assertThat( actualPig.version ).isEqualTo( 0 );
									  } )
							  )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
				.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 1 ) )
		);
	}

	@Test
	public void reactiveLockWithOptimisticIncrement(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( v -> getSessionFactory()
						.withTransaction( session -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( actualPig -> session
										.lock( actualPig, LockMode.OPTIMISTIC_FORCE_INCREMENT )
										.thenAccept( vv -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.OPTIMISTIC_FORCE_INCREMENT );
											assertThat( actualPig.version ).isEqualTo( 0 );
										} )
								)
						)
				)
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
				.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 1 ) )
		);
	}

	@Test
	public void reactiveLockWithIncrement(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
											  (session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
													  .thenCompose( actualPig -> session.lock(
																					actualPig,
																					LockMode.PESSIMISTIC_FORCE_INCREMENT
																			)
																			.thenAccept( vv -> {
																				assertThatPigsAreEqual( expectedPig, actualPig );
																				assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_FORCE_INCREMENT );
																				assertThat( actualPig.version ).isEqualTo( 1 );
																			} )
													  )
									  )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 1 ) )
		);
	}

	@Test
	public void reactiveFindWithOptimisticVerifyLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, transaction) -> session
										.find( GuineaPig.class, expectedPig.getId(), LockMode.OPTIMISTIC )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.OPTIMISTIC );
											assertThat( actualPig.version ).isEqualTo( 0 );
										} ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 0 ) )
		);
	}

	@Test
	public void reactiveLockWithOptimisticVerify(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction( (session, transaction) -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( actualPig -> session.lock( actualPig, LockMode.OPTIMISTIC )
										.thenAccept( vv -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.OPTIMISTIC );
											assertThat( actualPig.version ).isEqualTo( 0 );
										} ) ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 0 ) )
		);
	}

	@Test
	public void reactiveFindWithPessimisticRead(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, transaction) -> session
										// does a select ... for share
										.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_READ )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_READ );
											assertThat( actualPig.version ).isEqualTo( 0 );
										} ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 0 ) )
		);
	}

	@Test
	public void reactiveLockWithPessimisticRead(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, transaction) -> session
										// does a select ... for share
										.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.PESSIMISTIC_READ )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( expectedPig, actualPig );
													assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_READ );
													assertThat( actualPig.version ).isEqualTo( 0 );
												} ) ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 0 ) )
		);
	}

	@Test
	public void reactiveFindWithPessimisticWrite(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, transaction) -> session
										// does a select ... for update
										.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
											assertThat( actualPig.version ).isEqualTo( 0 );
										} ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 0 ) )
		);
	}

	@Test
	public void reactiveLockWithPessimisticWrite(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, transaction) -> session
										// does a select ... for update
										.find( GuineaPig.class, expectedPig.getId() )
										.thenCompose( actualPig -> session.lock( actualPig, LockMode.PESSIMISTIC_WRITE )
												.thenAccept( vv -> {
													assertThatPigsAreEqual( expectedPig, actualPig );
													assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
													assertThat( actualPig.version ).isEqualTo( 0 );
												} ) ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertThat( actualPig.version ).isEqualTo( 0 ) )
		);
	}

	@Test
	public void reactiveQueryWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, tx) -> session
										.createSelectionQuery( "from GuineaPig pig", GuineaPig.class )
										.setLockMode( LockModeType.PESSIMISTIC_WRITE )
										.getSingleResult()
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
										} ) ) )
		);
	}

	@Test
	public void reactiveQueryWithAliasedLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context, populateDB()
						.thenCompose( v -> getSessionFactory()
								.withTransaction( session -> session
										.createSelectionQuery( "from GuineaPig pig", GuineaPig.class )
										.setLockMode( "pig", LockMode.PESSIMISTIC_WRITE )
										.getSingleResult()
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
										} )
								)
						)
		);
	}

	@Test
	public void reactivePersist(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( new GuineaPig( 10, "Tulip" ) )
								.thenCompose( v -> s.flush() )
								.thenCompose( v -> s.close() )
						)
						.thenCompose( v -> selectNameFromId( 10 ) )
						.thenAccept( selectRes -> assertThat( selectRes ).isEqualTo( "Tulip" ) )
		);
	}

	@Test
	public void reactivePersistInTx(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> s
								.withTransaction( t -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
								.thenCompose( v -> s.close() ) )
						.thenCompose( vv -> selectNameFromId( 10 ) )
						.thenAccept( selectRes -> assertThat( selectRes ).isEqualTo( "Tulip" ) )
		);
	}

	@Test
	public void reactiveRollbackTx(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s
						.withTransaction( t -> s
								.persist( new GuineaPig( 10, "Tulip" ) )
								.thenCompose( v -> s.flush() )
								.thenAccept( v -> {
									throw new RuntimeException( "No Panic: This is just a test" );
								} )
						)
						.thenCompose( v -> s.close() )
				)
				.handle( (v, e) -> null )
				.thenCompose( vv -> selectNameFromId( 10 ) )
				.thenAccept( result -> assertThat( result ).isNull() )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s
						.withTransaction( t -> s
								.persist( new GuineaPig( 10, "Tulip" ) )
								.thenCompose( vv -> s.flush() )
								.thenAccept( vv -> t.markForRollback() )
						)
						.thenCompose( v -> s.close() )
				)
				.thenCompose( vv -> selectNameFromId( 10 ) )
				.thenAccept( result -> assertThat( result ).isNull() )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity(VertxTestContext context) {
		test( context, populateDB()
				.thenCompose( v -> selectNameFromId( 5 ) )
				.thenAccept( result -> assertThat( result ).isNotNull() )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> assertThrown( HibernateException.class, session.remove( new GuineaPig( 5, "Aloi" ) ) )
				)
				.thenAccept( t -> assertThat( t )
						.hasCauseInstanceOf( IllegalArgumentException.class )
						.hasMessageContaining( "unmanaged instance" )
				)
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(VertxTestContext context) {
		test( context, populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session
								.find( GuineaPig.class, 5 )
								.thenCompose( session::remove )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> selectNameFromId( 5 ) )
								.thenAccept( result -> assertThat( result ).isNull() ) )
		);
	}

	@Test
	public void reactiveUpdate(VertxTestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, 5 )
								.thenAccept( pig -> {
									assertThat( pig ).isNotNull();
									// Checking we are actually changing the name
									assertThat( pig.getName() ).isNotEqualTo( NEW_NAME );
									pig.setName( NEW_NAME );
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( name -> assertThat( name ).isEqualTo( NEW_NAME ) )
		);
	}

	@Test
	public void reactiveUpdateVersion(VertxTestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, 5 )
								.thenAccept( pig -> {
									assertThat( pig ).isNotNull();
									// Checking we are actually changing the name
									assertThat( pig.getName() ).isNotEqualTo( NEW_NAME );
									assertThat( pig.version ).isEqualTo( 0 );
									pig.setName( NEW_NAME );
									pig.version = 10; //ignored by Hibernate
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( GuineaPig.class, 5 )
								.thenAccept( pig -> assertThat( pig.version ).isEqualTo( 1 ) ) )
		);
	}

	@Test
	public void reactiveClose(VertxTestContext context) {
		test(
				context, openSession()
						.thenCompose( session -> {
							assertThat( session.isOpen() ).isTrue();
							return session.close()
									.thenAccept( v -> assertThat( session.isOpen() ).isFalse() );
						} )
		);
	}

	@Test
	@Disabled
	public void testSessionWithNativeAffectedEntities(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( 3, "Rorshach" );
		AffectedEntities affectsPigs = new AffectedEntities( GuineaPig.class );
		test(
				context,
				openSession().thenCompose( s -> s
						.persist( pig )
						.thenCompose( v -> s
								.createNativeQuery( "select * from pig where name=:n", GuineaPig.class, affectsPigs )
								.setParameter( "n", pig.name )
								.getResultList() )
						.thenAccept( list -> {
							assertThat( list ).isNotEmpty();
							assertThat( list.size() ).isEqualTo( 1 );
							assertThatPigsAreEqual( pig, list.get( 0 ) );
						} )
						.thenCompose( v -> s.find( GuineaPig.class, pig.id ) )
						.thenAccept( p -> {
							assertThatPigsAreEqual( pig, p );
							p.name = "X";
						} )
						.thenCompose( v -> s.createNativeQuery( "update pig set name='Y' where name='X'", affectsPigs ).executeUpdate() )
						.thenAccept( rows -> assertThat( rows ).isEqualTo( 1 ) )
						.thenCompose( v -> s.refresh( pig ) )
						.thenAccept( v -> assertThat( pig.name ).isEqualTo( "Y" ) )
						.thenAccept( v -> pig.name = "Z" )
						.thenCompose( v -> s.createNativeQuery( "delete from pig where name='Z'", affectsPigs ).executeUpdate() )
						.thenAccept( rows -> assertThat( rows ).isEqualTo( 1 ) )
						.thenCompose( v -> s.createNativeQuery( "select id from pig", affectsPigs ).getResultList() )
						.thenAccept( list -> assertThat( list ).isEmpty() ) )
		);
	}

	@Test
	public void testMetamodel() {
		EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity( GuineaPig.class );
		assertThat( pig ).isNotNull();
		assertThat( pig.getAttributes().size() ).isEqualTo( 3 );
		assertThat( pig.getName() ).isEqualTo( "GuineaPig" );
	}

	@Test
	void testFactory(VertxTestContext context) {
		test(
				context, getSessionFactory().withSession( session -> {
					session.getFactory().getCache().evictAll();
					session.getFactory().getMetamodel().entity( GuineaPig.class );
					session.getFactory().getCriteriaBuilder().createQuery( GuineaPig.class );
					session.getFactory().getStatistics().isStatisticsEnabled();
					return voidFuture();
				} )
		);
	}

	@Test
	public void testTransactionPropagation(VertxTestContext context) {
		test(
				context, getSessionFactory().withTransaction(
						(session, transaction) -> session
								.createSelectionQuery( "from GuineaPig", GuineaPig.class )
								.getResultList()
								.thenCompose( list -> {
									assertThat( session.currentTransaction() ).isNotNull();
									assertThat( session.currentTransaction().isMarkedForRollback() ).isFalse();
									session.currentTransaction().markForRollback();
									assertThat( session.currentTransaction().isMarkedForRollback() ).isTrue();
									assertThat( transaction.isMarkedForRollback() ).isTrue();
									return session.withTransaction( t -> {
										assertThat( t ).isEqualTo( transaction );
										assertThat( t.isMarkedForRollback() ).isTrue();
										return session.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList();
									} );
								} )
				)
		);
	}

	@Test
	public void testSessionPropagation(VertxTestContext context) {
		test(
				context, getSessionFactory().withSession( session -> {
					assertThat( session.isDefaultReadOnly() ).isFalse();
					session.setDefaultReadOnly( true );
					return session.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList()
							.thenCompose( list -> getSessionFactory().withSession( s -> {
								assertThat( s.isDefaultReadOnly() ).isTrue();
								return s.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList();
							} ) );
				} )
		);
	}

	@Test
	public void testDupeException(VertxTestContext context) {
		test(
				context,
				getSessionFactory()
						.withTransaction( (s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (s, t) -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						).handle( (i, t) -> {
							assertThat( t ).isNotNull();
							assertThat( t ).isInstanceOf( CompletionException.class );
							assertThat( t.getCause() ).isInstanceOf( PersistenceException.class );
							return null;
						} )
		);
	}

	@Test
	public void testExceptionInWithSession(VertxTestContext context) {
		final Stage.Session[] savedSession = new Stage.Session[1];
		test(
				context, getSessionFactory().withSession( session -> {
					assertThat( session.isOpen() ).isTrue();
					savedSession[0] = session;
					throw new RuntimeException( "No Panic: This is just a test" );
				} ).handle( (o, t) -> {
					assertThat( t ).isNotNull();
					assertThat( savedSession[0].isOpen() ).withFailMessage( "Session should be closed" ).isFalse();
					return null;
				} )
		);
	}

	@Test
	public void testExceptionInWithTransaction(VertxTestContext context) {
		final Stage.Session[] savedSession = new Stage.Session[1];
		test(
				context, getSessionFactory().withTransaction( (session, tx) -> {
					assertThat( session.isOpen() ).isTrue();
					savedSession[0] = session;
					throw new RuntimeException( "No Panic: This is just a test" );
				} ).handle( (o, t) -> {
					assertThat( t ).isNotNull();
					assertThat( savedSession[0].isOpen() ).withFailMessage( "Session should be closed" ).isFalse();
					return null;
				} )
		);
	}

	@Test
	public void testExceptionInWithStatelessSession(VertxTestContext context) {
		final Stage.StatelessSession[] savedSession = new Stage.StatelessSession[1];
		test(
				context, getSessionFactory().withStatelessSession( session -> {
					assertThat( session.isOpen() ).isTrue();
					savedSession[0] = session;
					throw new RuntimeException( "No Panic: This is just a test" );
				} ).handle( (o, t) -> {
					assertThat( t ).isNotNull();
					assertThat( savedSession[0].isOpen() ).withFailMessage( "Session should be closed" ).isFalse();
					return null;
				} )
		);
	}

	@Test
	public void testCreateSelectionQueryMultiple(VertxTestContext context) {
		final GuineaPig aloiPig = new GuineaPig( 10, "Aloi" );
		final GuineaPig bloiPig = new GuineaPig( 11, "Bloi" );

		test(
				context, openSession()
						.thenCompose( s -> s.withTransaction( t -> s.persist( aloiPig, bloiPig ) )
								.thenCompose( v -> openSession() )
								.thenCompose( session -> session
										.createSelectionQuery( "from GuineaPig", GuineaPig.class )
										.getResultList()
										.thenAccept( resultList -> assertThat( resultList ).containsExactlyInAnyOrder( aloiPig, bloiPig ) ) )
								.thenCompose( v -> openSession() )
								.thenCompose( session -> session
										.createSelectionQuery( "from GuineaPig", GuineaPig.class )
										.getResultList()
										.thenAccept( resultList -> assertThat( resultList ).containsExactlyInAnyOrder( aloiPig, bloiPig ) ) )
						)
		);
	}

	@Test
	public void testCreateSelectionQuerySingle(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 10, "Aloi" );
		test(
				context, openSession()
						.thenCompose( s -> s
								.withTransaction( t -> s.persist( new GuineaPig( 10, "Aloi" ) ) )
								.thenCompose( v -> openSession() )
								.thenCompose( session -> session
										.createSelectionQuery( "from GuineaPig", GuineaPig.class )
										.getSingleResult()
										.thenAccept( actualPig -> assertThatPigsAreEqual( expectedPig, actualPig ) ) )
								.thenCompose( v -> openSession() )
								.thenCompose( session -> session
										.createSelectionQuery( "from GuineaPig", GuineaPig.class )
										.getSingleResult()
										.thenAccept( actualPig -> assertThatPigsAreEqual( expectedPig, actualPig ) ) )
						)
		);
	}

	@Test
	public void testCreateSelectionQueryNull(VertxTestContext context) {
		test(
				context, openSession()
						.thenCompose( session -> session.createSelectionQuery( "from GuineaPig", GuineaPig.class )
								.getSingleResultOrNull()
								.thenAccept( result -> assertThat( result ).isNull() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createSelectionQuery( "from GuineaPig", GuineaPig.class )
								.getSingleResultOrNull()
								.thenAccept( result -> assertThat( result ).isNull() ) )
		);
	}

	@Test
	public void testCurrentSession(VertxTestContext context) {
		test(
				context, getSessionFactory()
						.withSession( s1 -> getSessionFactory()
								.withSession( s2 -> {
									assertThat( s2 ).isEqualTo( s1 );
									Stage.Session currentSession = getSessionFactory().getCurrentSession();
									assertThat( currentSession ).isNotNull();
									assertThat( currentSession.isOpen() ).isTrue();
									assertThat( currentSession ).isEqualTo( s1 );
									return voidFuture();
								} )
								// We closed s2, not s1
								.thenAccept( v -> assertThat( getSessionFactory().getCurrentSession() ).isNotNull() )
						)
						// Both sessions are closed now
						.thenAccept( v -> assertThat( getSessionFactory().getCurrentSession() ).isNull() )
		);
	}

	@Test
	public void testCurrentStatelessSession(VertxTestContext context) {
		test(
				context, getSessionFactory()
						.withStatelessSession( session -> getSessionFactory()
								.withStatelessSession( s -> {
									assertThat( s ).isEqualTo( session );
									Stage.StatelessSession currentSession = getSessionFactory().getCurrentStatelessSession();
									assertThat( currentSession ).isNotNull();
									assertThat( currentSession.isOpen() ).isTrue();
									assertThat( currentSession ).isEqualTo( session );
									return voidFuture();
								} )
								// We closed s2, not s1
								.thenAccept( v -> assertThat( getSessionFactory().getCurrentStatelessSession() ).isNotNull() )
						)
						// Both sessions are closed now
						.thenAccept( v -> assertThat( getSessionFactory().getCurrentStatelessSession() ).isNull() )
		);
	}

	private void assertThatPigsAreEqual(GuineaPig expected, GuineaPig actual) {
		assertThat( actual ).isNotNull();
		assertThat( actual.getId() ).isEqualTo( expected.getId() );
		assertThat( actual.getName() ).isEqualTo( expected.getName() );
	}

	@Entity(name = "GuineaPig")
	@Table(name = "pig")
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

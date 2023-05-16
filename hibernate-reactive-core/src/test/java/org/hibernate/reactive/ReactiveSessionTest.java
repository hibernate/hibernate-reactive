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
import java.util.concurrent.TimeUnit;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import jakarta.persistence.metamodel.EntityType;

@Timeout( value = 5, timeUnit = TimeUnit.MINUTES )
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

	@Override
	public CompletionStage<Void> cleanDb() {
		return getSessionFactory()
				.withTransaction( s -> s.createQuery( "delete from GuineaPig" ).executeUpdate()
						.thenCompose( CompletionStages::voidFuture ) );
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
									assertTrue( session.contains( actualPig ) );
									assertFalse( session.contains( expectedPig ) );
									assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
									session.detach( actualPig );
									assertFalse( session.contains( actualPig ) );
								} )
						)
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
						.thenCompose( v -> session.createQuery( "FROM GuineaPig", GuineaPig.class )
								// By not using .find() we check that there is only one entity in the db with getSingleResult()
								.getSingleResult() )
						.thenAccept( result -> assertThatPigsAreEqual( guineaPig, result ) )
				)
		);
	}

	@Test
	public void reactiveWithTransactionSession(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 61, "Mr. Peanutbutter" );
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( guineaPig ) )
				.thenCompose( v -> getSessionFactory()
						.withSession( session -> session.find( GuineaPig.class, guineaPig.getId() ) ) )
				.thenAccept( result -> assertThatPigsAreEqual( guineaPig, result ) )
		);
	}

	@Test
	public void reactiveWithTransactionStatelessSession(VertxTestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 61, "Mr. Peanutbutter" );
		test( context, getSessionFactory()
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
						.thenAccept( v -> assertFalse( session.contains( guineaPig ) ) )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( guineaPig, actualPig );
							assertTrue( session.contains( actualPig ) );
							assertFalse( session.contains( guineaPig ) );
							assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
							session.detach( actualPig );
							assertFalse( session.contains( actualPig ) );
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
		);
	}

	@Test
	public void reactiveFindWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );

		test( context, populateDB().thenCompose( v -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( expectedPig, actualPig );
							assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
						} )
				) )
		);
	}

	@Test
	public void reactiveFindRefreshWithLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( v -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session
										.refresh( pig, LockMode.PESSIMISTIC_WRITE )
										.thenAccept( vv -> {
											assertThatPigsAreEqual( expectedPig, pig );
											assertEquals( session.getLockMode( pig ), LockMode.PESSIMISTIC_WRITE );
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
												assertEquals( expectedPig.name, pig.name );
												assertTrue( session.isReadOnly( pig ) );
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
												assertEquals( "XXXX", pig.name );
												assertFalse( session.isReadOnly( pig ) );
											} );
								} )
						)
		);
	}

	@Test
	public void reactiveFindThenUpgradeLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( unused -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session
										.lock( pig, LockMode.PESSIMISTIC_READ )
										.thenAccept( v -> {
											assertThatPigsAreEqual( expectedPig, pig );
											assertEquals(
													session.getLockMode( pig ),
													LockMode.PESSIMISTIC_READ
											);
										} )
								)
						)
				)
		);
	}

	@Test
	public void reactiveFindThenWriteLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB().thenCompose( v -> getSessionFactory()
				.withTransaction( (session, tx) -> session
						.find( GuineaPig.class, expectedPig.getId() )
						.thenCompose( pig -> session
								.lock( pig, LockMode.PESSIMISTIC_WRITE )
								.thenAccept( vv -> {
									assertThatPigsAreEqual( expectedPig, pig );
									assertEquals( session.getLockMode( pig ), LockMode.PESSIMISTIC_WRITE );
									assertEquals( pig.version, 0 );
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
									assertEquals(
											session.getLockMode( actualPig ),
											LockMode.PESSIMISTIC_FORCE_INCREMENT
									);
									assertEquals( actualPig.version, 1 );
								} )
								.thenCompose( v -> session.createQuery( "select version from GuineaPig" )
										.getSingleResult() )
								.thenAccept( version -> assertEquals( 1, version ) )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.thenCompose( pig -> session.lock( pig, LockMode.PESSIMISTIC_FORCE_INCREMENT )
										.thenApply( v -> pig ) )
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( expectedPig, actualPig );
									assertEquals(
											session.getLockMode( actualPig ),
											LockMode.PESSIMISTIC_FORCE_INCREMENT
									);
									assertEquals( actualPig.version, 2 );
								} )
								.thenCompose( v -> session.createQuery( "select version from GuineaPig" )
										.getSingleResult() )
								.thenAccept( version -> assertEquals( 2, version ) )
						)
		);
	}

	@Test
	public void reactiveFindWithPessimisticIncrementLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
								(session, transaction) -> session.find(
										GuineaPig.class,
										expectedPig.getId(),
										LockMode.PESSIMISTIC_FORCE_INCREMENT )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertEquals(
													session.getLockMode( actualPig ),
													LockMode.PESSIMISTIC_FORCE_INCREMENT ); // grrr, lame
											assertEquals( 1, actualPig.version );
										} ) )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 1, actualPig.version ) )
		);
	}

	@Test
	public void reactiveFindWithOptimisticIncrementLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
											  (session, transaction) -> session.find(
															  GuineaPig.class,
															  expectedPig.getId(),
															  LockMode.OPTIMISTIC_FORCE_INCREMENT
													  )
													  .thenAccept( actualPig -> {
														  assertThatPigsAreEqual( expectedPig, actualPig );
														  assertEquals(
																  session.getLockMode( actualPig ),
																  LockMode.OPTIMISTIC_FORCE_INCREMENT
														  );
														  assertEquals( 0, actualPig.version );
													  } )
									  )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 1, actualPig.version ) )
		);
	}

	@Test
	public void reactiveLockWithOptimisticIncrement(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory().withTransaction(
											  (session, transaction) -> session.find( GuineaPig.class, expectedPig.getId() )
													  .thenCompose( actualPig -> session.lock(
																					actualPig,
																					LockMode.OPTIMISTIC_FORCE_INCREMENT
																			)
																			.thenAccept( vv -> {
																				assertThatPigsAreEqual( expectedPig, actualPig );
																				assertEquals(
																						session.getLockMode( actualPig ),
																						LockMode.OPTIMISTIC_FORCE_INCREMENT
																				);
																				assertEquals( 0, actualPig.version );
																			} )
													  )
									  )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 1, actualPig.version ) )
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
																				assertEquals(
																						session.getLockMode( actualPig ),
																						LockMode.PESSIMISTIC_FORCE_INCREMENT
																				);
																				assertEquals( 1, actualPig.version );
																			} )
													  )
									  )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 1, actualPig.version ) )
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
											assertEquals( session.getLockMode( actualPig ), LockMode.OPTIMISTIC );
											assertEquals( 0, actualPig.version );
										} ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 0, actualPig.version ) ) );
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
											assertEquals( session.getLockMode( actualPig ), LockMode.OPTIMISTIC );
											assertEquals( 0, actualPig.version );
										} ) ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 0, actualPig.version ) )
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
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
											assertEquals( 0, actualPig.version );
										} ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 0, actualPig.version ) )
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
													assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_READ );
													assertEquals( 0, actualPig.version );
												} ) ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 0, actualPig.version ) )
		);
	}

	@Test
	public void reactiveFindWithPessimisticWrite(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> getSessionFactory ()
								.withTransaction( (session, transaction) -> session
										// does a select ... for update
										.find( GuineaPig.class, expectedPig.getId(), LockMode.PESSIMISTIC_WRITE )
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
											assertEquals( 0, actualPig.version );
										} ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 0, actualPig.version ) )
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
													assertEquals(
															session.getLockMode( actualPig ),
															LockMode.PESSIMISTIC_WRITE );
													assertEquals( 0, actualPig.version );
												} ) ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> assertEquals( 0, actualPig.version ) )
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
										.createQuery( "from GuineaPig pig", GuineaPig.class )
										.setLockMode( LockModeType.PESSIMISTIC_WRITE )
										.getSingleResult()
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertEquals(
													session.getLockMode( actualPig ),
													LockMode.PESSIMISTIC_WRITE );
										} ) ) )
		);
	}

	@Test
	public void reactiveQueryWithAliasedLock(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose(
						v -> getSessionFactory().withTransaction(
								(session, tx) -> session.createQuery( "from GuineaPig pig", GuineaPig.class )
										.setLockMode( "pig", LockMode.PESSIMISTIC_WRITE )
										.getSingleResult()
										.thenAccept( actualPig -> {
											assertThatPigsAreEqual( expectedPig, actualPig );
											assertEquals(
													session.getLockMode( actualPig ),
													LockMode.PESSIMISTIC_WRITE
											);
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
						.thenAccept( selectRes -> assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersistInTx(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> s
								.withTransaction( t -> s.persist( new GuineaPig( 10, "Tulip" ) ))
								.thenCompose( v -> s.close() ) )
						.thenCompose( vv -> selectNameFromId( 10 ) )
						.thenAccept( selectRes -> assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRollbackTx(VertxTestContext context) {
		test(
				context,
				openSession()
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
						.thenAccept( Assertions::assertNull)
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
				.thenAccept( Assertions::assertNull )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity(VertxTestContext context) {
		test(
				context,
				populateDB()
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( Assertions::assertNotNull )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.remove( new GuineaPig( 5, "Aloi" ) )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( Assertions::assertNull )
						.handle( (r, e) -> {
								Assertions.assertInstanceOf( HibernateException.class, e.getCause() );
								return CompletionStages.voidFuture();
						} )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(VertxTestContext context) {
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session ->
											  session.find( GuineaPig.class, 5 )
													  .thenCompose( session::remove )
													  .thenCompose( v -> session.flush() )
													  .thenCompose( v -> selectNameFromId( 5 ) )
													  .thenAccept( Assertions::assertNull ) )
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
									assertNotNull( pig );
									// Checking we are actually changing the name
									Assertions.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( name -> assertEquals( NEW_NAME, name ) )
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
									assertNotNull( pig );
									// Checking we are actually changing the name
									Assertions.assertNotEquals( pig.getName(), NEW_NAME );
									assertEquals( pig.version, 0 );
									pig.setName( NEW_NAME );
									pig.version = 10; //ignored by Hibernate
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> session.close() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( GuineaPig.class, 5 )
								.thenAccept( pig -> assertEquals( pig.version, 1 ) ) )
		);
	}

	@Test
	public void reactiveClose(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> {
					assertTrue( session.isOpen() );
					return session.close()
							.thenAccept( v -> assertFalse( session.isOpen() ) );
				} )
		);
	}

	@Test
	public void testSessionWithNativeAffectedEntities(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( 3, "Rorshach" );
		AffectedEntities affectsPigs = new AffectedEntities( GuineaPig.class );
		test(
				context,
				openSession().thenCompose( s -> s
						.persist( pig )
						.thenCompose( v -> s.createNativeQuery( "select * from pig where name=:n", GuineaPig.class, affectsPigs )
								.setParameter( "n", pig.name )
								.getResultList() )
						.thenAccept( list -> {
							assertFalse( list.isEmpty() );
							assertEquals( 1, list.size() );
							assertThatPigsAreEqual( pig, list.get( 0 ) );
						} )
						.thenCompose( v -> s.find( GuineaPig.class, pig.id ) )
						.thenAccept( p -> {
							assertThatPigsAreEqual( pig, p );
							p.name = "X";
						} )
						.thenCompose( v -> s.createNativeQuery( "update pig set name='Y' where name='X'", affectsPigs ).executeUpdate() )
						.thenAccept( rows -> assertEquals( 1, rows ) )
						.thenCompose( v -> s.refresh( pig ) )
						.thenAccept( v -> assertEquals( pig.name, "Y" ) )
						.thenAccept( v -> pig.name = "Z" )
						.thenCompose( v -> s.createNativeQuery( "delete from pig where name='Z'", affectsPigs ).executeUpdate() )
						.thenAccept( rows -> assertEquals( 1, rows ) )
						.thenCompose( v -> s.createNativeQuery( "select id from pig", affectsPigs ).getResultList() )
						.thenAccept( list -> assertTrue( list.isEmpty() ) ) )
		);
	}

	@Test
	public void testMetamodel(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> {
			EntityType<GuineaPig> pig = getSessionFactory().getMetamodel().entity( GuineaPig.class );
			Assertions.assertNotNull( pig );
			assertEquals( 3, pig.getAttributes().size() );
			assertEquals( "GuineaPig", pig.getName() );
			return CompletionStages.voidFuture();
		} ) );
	}

	@Test
	public void testTransactionPropagation(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction(
				(session, transaction) -> session.createQuery( "from GuineaPig" ).getResultList()
						.thenCompose( list -> {
							assertNotNull( session.currentTransaction() );
							assertFalse( session.currentTransaction().isMarkedForRollback() );
							session.currentTransaction().markForRollback();
							assertTrue( session.currentTransaction().isMarkedForRollback() );
							assertTrue( transaction.isMarkedForRollback() );
							return session.withTransaction( t -> {
								assertEquals( t, transaction );
								assertTrue( t.isMarkedForRollback() );
								return session.createQuery( "from GuineaPig" ).getResultList();
							} );
						} )
		) );
	}

	@Test
	public void testSessionPropagation(VertxTestContext context) {
		test( context, getSessionFactory().withSession( session -> {
			assertFalse( session.isDefaultReadOnly() );
			session.setDefaultReadOnly( true );
			return session.createQuery( "from GuineaPig" ).getResultList()
					.thenCompose( list -> getSessionFactory().withSession( s -> {
						assertTrue( s.isDefaultReadOnly() );
						return s.createQuery( "from GuineaPig" ).getResultList();
					} ) );
		} ) );
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
							Assertions.assertNotNull( t );
							assertTrue( t instanceof CompletionException );
							assertTrue( t.getCause() instanceof PersistenceException );
							return null;
						} )
		);
	}

	@Test
	public void testExceptionInWithSession(VertxTestContext context) {
		final Stage.Session[] savedSession = new Stage.Session[1];
		test( context, getSessionFactory().withSession( session -> {
			assertTrue( session.isOpen() );
			savedSession[0] = session;
			throw new RuntimeException( "No Panic: This is just a test" );
		} ).handle( (o, t) -> {
			assertNotNull( t );
			assertFalse( savedSession[0].isOpen(), "Session should be closed" );
			return null;
		} ) );
	}

	@Test
	public void testExceptionInWithTransaction(VertxTestContext context) {
		final Stage.Session[] savedSession = new Stage.Session[1];
		test( context, getSessionFactory().withTransaction( (session, tx) -> {
			assertTrue( session.isOpen() );
			savedSession[0] = session;
			throw new RuntimeException( "No Panic: This is just a test" );
		} ).handle( (o, t) -> {
			assertNotNull( t );
			assertFalse( savedSession[0].isOpen(), "Session should be closed" );
			return null;
		} ) );
	}

	@Test
	public void testExceptionInWithStatelessSession(VertxTestContext context) {
		final Stage.StatelessSession[] savedSession = new Stage.StatelessSession[1];
		test( context, getSessionFactory().withStatelessSession( session -> {
			assertTrue( session.isOpen() );
			savedSession[0] = session;
			throw new RuntimeException( "No Panic: This is just a test" );
		} ).handle( (o, t) -> {
			assertNotNull( t );
			assertFalse( savedSession[0].isOpen(), "Session should be closed" );
			return null;
		} ) );
	}

	@Test
	public void testCreateSelectionQueryMultiple(VertxTestContext context) {
		final GuineaPig aloiPig = new GuineaPig( 10, "Aloi" );
		final GuineaPig bloiPig = new GuineaPig( 11, "Bloi" );

		test( context, openSession()
				.thenCompose( s -> s.withTransaction( t -> s.persist( aloiPig, bloiPig ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( "from GuineaPig", GuineaPig.class )
								.getResultList()
								.thenAccept( resultList -> assertThat( resultList ).containsExactlyInAnyOrder( aloiPig, bloiPig ) )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session
								.createSelectionQuery( "from GuineaPig" )
								.getResultList()
								.thenAccept( resultList -> assertThat( resultList ).containsExactlyInAnyOrder( aloiPig, bloiPig ) ) ) )
		);
	}

	@Test
	public void testCreateSelectionQuerySingle(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 10, "Aloi" );
		test( context, openSession()
				.thenCompose( s -> s
						.withTransaction( t -> s.persist( new GuineaPig( 10, "Aloi" ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( "from GuineaPig", GuineaPig.class )
								.getSingleResult()
								.thenAccept( actualPig -> assertThatPigsAreEqual( expectedPig, actualPig ) ) )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createSelectionQuery( "from GuineaPig" )
								.getSingleResult()
								.thenAccept( actualPig -> assertThatPigsAreEqual( expectedPig, (GuineaPig) actualPig ) ) )
				)
		);
	}

	@Test
	public void testCreateSelectionQueryNull(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createQuery( "from GuineaPig", GuineaPig.class )
						.getSingleResultOrNull()
						.thenAccept( Assertions::assertNull ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.createSelectionQuery( "from GuineaPig" )
						.getSingleResultOrNull()
						.thenAccept( Assertions::assertNull ) )
		);
	}

	private void assertThatPigsAreEqual(GuineaPig expected, GuineaPig actual) {
		assertNotNull( actual );
		assertEquals( expected.getId(), actual.getId() );
		assertEquals( expected.getName(), actual.getName() );
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

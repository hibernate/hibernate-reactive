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
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.metamodel.EntityType;
import java.util.Objects;

public class MutinySessionTest extends BaseMutinyTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	private Uni<Integer> populateDB() {
		return connection().flatMap(
				conn -> Uni.createFrom().completionStage( conn.update( "INSERT INTO Pig (id, name) VALUES (5, 'Aloi')" ) )
		);
	}
//
//	private Uni<Integer> cleanDB() {
//		return connection().flatMap( conn -> Uni.createFrom().completionStage( conn.update( "DELETE FROM Pig" ) ) );
//	}

	private Uni<String> selectNameFromId(Integer id) {
		return connection().flatMap(
				connection -> Uni.createFrom().completionStage(
						connection.select(
								DatabaseConfiguration.statement( "SELECT name FROM Pig WHERE id = ", "" ),
								new Object[]{id})
								.thenApply(
										rowSet -> {
											if (rowSet.size() == 1) {
												// Only one result
												return (String) rowSet.next()[0];
											} else if (rowSet.size() > 1) {
												throw new AssertionError("More than one result returned: " + rowSet.size());
											} else {
												// Size 0
												return null;
											}
										})
				) );
	}

	@Test
	public void reactiveFind1(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.onItem().produceUni( i -> openSession() )
						.onItem().produceUni( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.onItem().invoke( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertTrue( session.contains( actualPig ) );
									context.assertFalse( session.contains( expectedPig ) );
									context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
									session.detach( actualPig );
									context.assertFalse( session.contains( actualPig ) );
								} ) )
		);
	}

	@Test
	public void reactiveFind2(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.flatMap( i -> openSession() )
						.flatMap( session -> session.find( GuineaPig.class, expectedPig.getId() )
								.onItem().invoke( actualPig -> {
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
	public void reactivePersist1(TestContext context) {
		test(
				context,
				openSession()
						.onItem().produceUni( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.onItem().produceUni( s -> s.flush() )
						.onItem().produceUni( v -> selectNameFromId( 10 ) )
						.onItem().invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersist2(TestContext context) {
		test(
				context,
				openSession()
						.flatMap( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.flatMap( s -> s.flush() )
						.flatMap( v -> selectNameFromId( 10 ) )
						.map( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactivePersistInTx(TestContext context) {
		test(
				context,
				openSession()
						.flatMap(
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
//												.thenCompose( vv -> s.flush() )
								)
						)
						.flatMap( vv -> selectNameFromId( 10 ) )
						.map( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRollbackTx(TestContext context) {
		test(
				context,
				openSession()
						.flatMap(
								s -> s.withTransaction(
										t -> s.persist( new ReactiveSessionTest.GuineaPig( 10, "Tulip" ) )
												.flatMap( vv -> s.flush() )
												.map( vv -> {
													throw new RuntimeException();
												})
								)
						)
						.on().failure().recoverWithItem((Object)null)
						.flatMap( vv -> selectNameFromId( 10 ) )
						.map( context::assertNull )
		);
	}

	@Test
	public void reactiveMarkedRollbackTx(TestContext context) {
		test(
				context,
				openSession()
						.flatMap(
								s -> s.withTransaction(
										t -> s.persist( new GuineaPig( 10, "Tulip" ) )
												.flatMap( vv -> s.flush() )
												.map( vv -> { t.markForRollback(); return null; } )
								)
						)
						.flatMap( vv -> selectNameFromId( 10 ) )
						.map( context::assertNull )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().produceUni( v -> selectNameFromId( 5 ) )
						.onItem().invoke( name -> context.assertNotNull( name ) )
						.onItem().produceUni( v -> openSession() )
						.onItem().produceUni( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().produceUni( session -> session.flush() )
						.onItem().produceUni( v -> selectNameFromId( 5 ) )
						.onItem().invoke( ret -> context.assertNull( ret ) )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.flatMap( v -> selectNameFromId( 5 ) )
						.map( name -> context.assertNotNull( name ) )
						.flatMap( v -> openSession() )
						.flatMap( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.flatMap( session -> session.flush() )
						.flatMap( v -> selectNameFromId( 5 ) )
						.map( ret -> context.assertNull( ret ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().produceUni( v -> openSession() )
						.onItem().produceUni( session ->
								session.find( GuineaPig.class, 5 )
										.onItem().produceUni( aloi -> session.remove( aloi ) )
										.onItem().produceUni( v -> session.flush() )
										.onItem().produceUni( v -> selectNameFromId( 5 ) )
										.onItem().invoke( ret -> context.assertNull( ret ) ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.flatMap( v -> openSession() )
						.flatMap( session ->
							session.find( GuineaPig.class, 5 )
								.flatMap( aloi -> session.remove( aloi ) )
								.flatMap( v -> session.flush() )
								.flatMap( v -> selectNameFromId( 5 ) )
								.map( ret -> context.assertNull( ret ) ) )
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.flatMap( v -> openSession() )
						.flatMap( session ->
							session.find( GuineaPig.class, 5 )
								.map( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
									return null;
								} )
								.flatMap( v -> session.flush() )
								.flatMap( v -> selectNameFromId( 5 ) )
								.map( name -> context.assertEquals( NEW_NAME, name ) ) )
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

package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.LockMode;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.metamodel.EntityType;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static org.junit.Assume.assumeTrue;

public class ReactiveSessionTest extends BaseReactiveTest {
	
	@BeforeClass
	public static void beforeAll() {
		// TODO: @AGG investigate this test class for DB2
		assumeTrue(DatabaseConfiguration.dbType() != DBType.DB2);
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
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
		cleanDB()
				.whenComplete( (res, err) -> {
					// in case cleanDB() fails we
					// stll have to close the factory
					try {
						super.after(context);
					}
					finally {
						context.assertNull( err );
					}
				} )
				.whenComplete( (res, err) -> {
					// in case cleanDB() worked but
					// SessionFactory didn't close
					context.assertNull( err );
				} );
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
						)
		);
	}

	@Test
	public void reactiveFindWithLock(TestContext context) {
		// TODO @AGG
		// The DB2 driver does not yet support a few types (BigDecimal, BigInteger, LocalTime)
		// so we need to keep a separate copy around for testing DB2 (DB2BasicTest)
		assumeTrue( DatabaseConfiguration.dbType() != DBType.DB2 );
		
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
						)
		);
	}

	@Test
	public void reactiveQueryWithLock(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( "from GuineaPig pig", GuineaPig.class).setLockMode("pig", LockMode.PESSIMISTIC_WRITE )
								.getSingleResult()
								.thenAccept( actualPig -> {
									assertThatPigsAreEqual( context, expectedPig, actualPig );
									context.assertEquals( session.getLockMode( actualPig ), LockMode.PESSIMISTIC_WRITE );
								} )
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
//												.thenCompose( vv -> s.flush() )
								)
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
												.thenCompose( vv -> s.flush() )
												.thenAccept( vv -> {
													throw new RuntimeException();
												})
								)
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
						.thenCompose( session ->
							session.find( GuineaPig.class, 5 )
								.thenAccept( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> selectNameFromId( 5 ) )
								.thenAccept( name -> context.assertEquals( NEW_NAME, name ) ) )
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

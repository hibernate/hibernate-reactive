package org.hibernate.reactive;

import io.vertx.sqlclient.Tuple;
import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public class CompositeIdTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	private CompletionStage<Integer> populateDB() {
		return connection().update( "INSERT INTO Pig (id, name, weight) VALUES (5, 'Aloi', 100)" );
	}

	private CompletionStage<Integer> cleanDB() {
		return connection().update( "DELETE FROM Pig" );
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
		return connection().preparedQuery(
				"SELECT name FROM Pig WHERE id = $1", Tuple.of( id ) ).thenApply(
				rowSet -> {
					if ( rowSet.size() == 1 ) {
						// Only one result
						return rowSet.iterator().next().getString( 0 );
					}
					else if ( rowSet.size() > 1 ) {
						throw new AssertionError( "More than one result returned: " + rowSet.size() );
					}
					else {
						// Size 0
						return null;
					}
				} );
	}

	private CompletionStage<Double> selectWeightFromId(Integer id) {
		return connection().preparedQuery(
				"SELECT weight FROM Pig WHERE id = $1", Tuple.of( id ) ).thenApply(
				rowSet -> {
					if ( rowSet.size() == 1 ) {
						// Only one result
						return rowSet.iterator().next().getDouble( 0 );
					}
					else if ( rowSet.size() > 1 ) {
						throw new AssertionError( "More than one result returned: " + rowSet.size() );
					}
					else {
						// Size 0
						return null;
					}
				} );
	}

	@Test
	public void reactiveFind(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.find( GuineaPig.class, new Pig(5, "Aloi") ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( context, expectedPig, actualPig );
						} )
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
							session.find( GuineaPig.class, new Pig(5, "Aloi") )
								.thenCompose( aloi -> session.remove( aloi ) )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> selectNameFromId( 5 ) )
								.thenAccept( context::assertNull ) )
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final double NEW_WEIGHT = 200.0;
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( session ->
							session.find( GuineaPig.class, new Pig(5, "Aloi") )
								.thenAccept( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getWeight(), NEW_WEIGHT );
									pig.setWeight( NEW_WEIGHT );
								} )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> selectWeightFromId( 5 ) )
								.thenAccept( w -> context.assertEquals( NEW_WEIGHT, w ) ) )
		);
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
		context.assertEquals( expected.getWeight(), actual.getWeight() );
	}

	static final class Pig implements Serializable {
		@Id private Integer id;
		@Id private String name;

		public Pig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		Pig() {}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Pig pig = (Pig) o;
			return id.equals(pig.id) &&
					name.equals(pig.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}
	}

	@Entity
	@Table(name="Pig")
	@IdClass(Pig.class)
	public static class GuineaPig implements Serializable {
		@Id private Integer id;
		@Id private String name;

		private double weight = 100.0;

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

		public double getWeight() {
			return weight;
		}

		public void setWeight(double weight) {
			this.weight = weight;
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

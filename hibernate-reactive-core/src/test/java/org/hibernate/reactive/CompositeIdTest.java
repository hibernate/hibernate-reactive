/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

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

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class CompositeIdTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	private CompletionStage<Void> populateDB() {
		return getSessionFactory()
				.withSession(
						session -> session.persist( new GuineaPig(5, "Aloi", 100) )
							.thenCompose( v -> session.flush() )
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

	private CompletionStage<Double> selectWeightFromId(Integer id) {
		return getSessionFactory().withSession(
				session -> session.createQuery("SELECT weight FROM GuineaPig WHERE id = " + id )
						.getResultList()
						.thenApply(
								rowSet -> {
									switch ( rowSet.size() ) {
										case 0:
											return null;
										case 1:
											return (Double) rowSet.get(0);
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
						.thenApply( v -> openSession() )
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
				completedFuture( openSession() )
						.thenCompose( s -> s.persist( new GuineaPig( 10, "Tulip" ) )
								.thenCompose( v -> s.flush() )
								.whenComplete( (v,e) -> s.close() )
						)
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
						.thenApply( v -> openSession() )
						.thenCompose( session -> session.remove( new GuineaPig( 5, "Aloi" ) )
								.thenCompose( v -> session.flush() )
								.whenComplete( (v, err) -> session.close() )
						)
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( context::assertNull )
						.handle((r, e) -> context.assertNotNull(e))
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(TestContext context) {
		test(
				context,
				populateDB()
						.thenApply( v -> openSession() )
						.thenCompose( session ->
							session.find( GuineaPig.class, new Pig(5, "Aloi") )
								.thenCompose( aloi -> session.remove( aloi ) )
								.thenCompose( v -> session.flush() )
								.thenCompose( v -> selectNameFromId( 5 ) )
								.thenAccept( context::assertNull )
								.whenComplete( (v, err) -> session.close() )
						)
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final double NEW_WEIGHT = 200.0;
		test(
				context,
				populateDB()
						.thenApply( v -> openSession() )
						.thenCompose( session ->
							session.find( GuineaPig.class, new Pig(5, "Aloi") )
								.thenAccept( pig -> {
									context.assertNotNull( pig );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getWeight(), NEW_WEIGHT );
									pig.setWeight( NEW_WEIGHT );
								} )
								.thenCompose( v -> session.flush() )
								.whenComplete( (v, err) -> session.close() )
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

	@Entity(name="GuineaPig")
	@Table(name="Pig")
	@IdClass(Pig.class)
	public static class GuineaPig implements Serializable {
		@Id private Integer id;
		@Id private String name;

		private double weight = 100.0;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name, int weight) {
			this.id = id;
			this.name = name;
			this.weight = weight;
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

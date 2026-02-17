/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;


import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class CompositeIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	private CompletionStage<Void> populateDB() {
		return getSessionFactory()
				.withTransaction( session -> session
						.persist( new GuineaPig( 5, "Aloi", 100 ) )
				);
	}

	private CompletionStage<String> selectNameFromId(Integer id) {
		return getSessionFactory().withSession( session -> selectNameFromId( session, id ) );
	}

	private CompletionStage<String> selectNameFromId(Stage.Session session, Integer id) {
		return session.createSelectionQuery( "SELECT name FROM GuineaPig WHERE id = " + id, String.class )
				.getResultList()
				.thenApply( CompositeIdTest::nameFromResult );
	}

	private static String nameFromResult(List<String> rowSet) {
		switch ( rowSet.size() ) {
			case 0:
				return null;
			case 1:
				return rowSet.get( 0 );
			default:
				throw new AssertionError( "More than one result returned: " + rowSet.size() );
		}
	}

	private CompletionStage<Double> selectWeightFromId(Integer id) {
		return getSessionFactory().withSession( session -> session
				.createSelectionQuery( "SELECT weight FROM GuineaPig WHERE id = " + id, Double.class )
				.getResultList()
				.thenApply( CompositeIdTest::weightFromResult )
		);
	}

	private static Double weightFromResult(List<Double> rowSet) {
		switch ( rowSet.size() ) {
			case 0:
				return null;
			case 1:
				return rowSet.get( 0 );
			default:
				throw new AssertionError( "More than one result returned: " + rowSet.size() );
		}
	}

	@Test
	public void reactiveFind(VertxTestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test( context, populateDB()
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( GuineaPig.class, new Pig( 5, "Aloi" ) ) )
				.thenAccept( actualPig -> assertThatPigsAreEqual( context, expectedPig, actualPig ) )
		);
	}

	@Test
	public void reactivePersist(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s
						.persist( new GuineaPig( 10, "Tulip" ) )
						.thenCompose( v -> s.flush() )
				)
				.thenCompose( v -> selectNameFromId( 10 ) )
				.thenAccept( selectRes -> assertThat( selectRes ).isEqualTo( "Tulip" ) )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity(VertxTestContext context) {
		test( context, populateDB()
				.thenCompose( v -> selectNameFromId( 5 ) )
				.thenAccept( Assertions::assertNotNull )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.remove( new GuineaPig( 5, "Aloi" ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> session.close() )
				)
				.thenCompose( v -> selectNameFromId( 5 ) )
				.thenAccept( Assertions::assertNull )
				.handle( (r, e) -> {
					assertThat( e ).isNotNull();
					return r;
				} )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(VertxTestContext context) {
		test( context, populateDB()
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.find( GuineaPig.class, new Pig( 5, "Aloi" ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> selectNameFromId( session, 5 ) )
						.thenAccept( Assertions::assertNull )
				)
		);
	}

	@Test
	public void reactiveUpdate(VertxTestContext context) {
		final double NEW_WEIGHT = 200.0;
		test( context, populateDB()
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session
						.find( GuineaPig.class, new Pig( 5, "Aloi" ) )
						.thenAccept( pig -> {
							assertThat( pig ).isNotNull();
							// Checking we are actually changing the name
							assertThat( pig.getWeight() ).isNotEqualTo( NEW_WEIGHT );
							pig.setWeight( NEW_WEIGHT );
						} )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> session.close() )
						.thenCompose( v -> selectWeightFromId( 5 ) )
						.thenAccept( w -> assertThat( w ).isEqualTo( NEW_WEIGHT ) ) )
		);
	}

	private void assertThatPigsAreEqual(VertxTestContext context, GuineaPig expected, GuineaPig actual) {
		assertThat( actual ).isNotNull();
		assertThat( actual.getId() ).isEqualTo( expected.getId() );
		assertThat( actual.getName() ).isEqualTo( expected.getName() );
		assertThat( actual.getWeight() ).isEqualTo( expected.getWeight() );
	}

	static final class Pig implements Serializable {
		private Integer id;
		private String name;

		public Pig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		Pig() {
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Pig pig = (Pig) o;
			return id.equals( pig.id ) &&
					name.equals( pig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, name );
		}
	}

	@Entity(name = "GuineaPig")
	@Table(name = "Pig")
	@IdClass(Pig.class)
	public static class GuineaPig implements Serializable {
		@Id
		private Integer id;
		@Id
		private String name;

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

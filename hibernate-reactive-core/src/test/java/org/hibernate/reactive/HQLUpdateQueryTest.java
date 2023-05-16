/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HQLUpdateQueryTest extends BaseReactiveTest {

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class );
	}

	public CompletionStage<Void> populateDb() {
		return openSession()
				.thenCompose( s -> s.persist( spelt )
				.thenCompose( v -> s.persist( rye ) )
				.thenCompose( v -> s.persist( almond ) )
				.thenCompose( v -> s.flush() )
		);
	}

	@Test
	public void testUpdateQuery(VertxTestContext context) {
		String updatedDescription =  "Most rye breads use a mix of rye and wheat flours";
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenApply( s -> s.createQuery( "UPDATE Flour SET description = '" + updatedDescription + "' WHERE id = " + rye.getId()  ) ) )
						.thenCompose( qr -> {
							assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> assertEquals( 1, resultCount ))
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, rye.getId() ) )
						.thenAccept( result -> assertEquals( updatedDescription, result.getDescription() ) )
		);
	}

	@Test
	public void testUpdateQueryWithParameters(VertxTestContext context) {
		String updatedDescription =  "Most rye breads use a mix of rye and wheat flours";
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenApply( s -> s.createQuery( "UPDATE Flour SET description = :updatedDescription WHERE id = :id" )
								.setParameter("updatedDescription", updatedDescription)
								.setParameter("id", rye.getId()) ) )
						.thenCompose( qr -> {
							assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> assertEquals( 1, resultCount ))
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, rye.getId() ) )
						.thenAccept( result -> assertEquals( updatedDescription, result.getDescription() ) )
		);
	}

	@Test
	public void testInsertQuery(VertxTestContext context) {
		Flour chestnut = new Flour( 777, "Chestnut", "The original ingredient for polenta", "gluten-free" );
		StringBuilder insertQueryBuilder = new StringBuilder( "insert into Flour(id, name, description, type) select " );
		insertQueryBuilder.append( chestnut.getId() ).append( ", " );
		insertQueryBuilder.append( "'" ).append( chestnut.getName() ).append( "', " );
		insertQueryBuilder.append( "'" ).append( chestnut.getDescription() ).append( "', " );
		insertQueryBuilder.append( "'" ).append( chestnut.getType() ).append( "' " );
		insertQueryBuilder.append( " from Flour where id = " ).append( rye.getId() );
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenApply( s -> s.createQuery( insertQueryBuilder.toString() ) ) )
						.thenCompose( qr -> {
							assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> assertEquals( 1, resultCount ) )
						// Check if it's really been inserted
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, chestnut.getId() ) )
						.thenAccept( result -> assertEquals( chestnut, result ) )
						// Cleanup db
						.thenCompose( v -> openSession() )
						.thenAccept( s -> s.remove( chestnut ) )
		);
	}

	@Test
	public void testDeleteQuery(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenApply( s -> s.createQuery( "DELETE FROM Flour WHERE id = " + rye.getId() ) ) )
						.thenCompose( qr -> {
							assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> assertEquals( 1, resultCount ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, rye.getId() ) )
						.thenAccept( Assertions::assertNull )
		);
	}

	@Entity(name = "Flour")
	@Table(name = "Flour")
	public static class Flour {
		@Id
		private Integer id;
		private String name;
		private String description;
		private String type;

		public Flour() {
		}

		public Flour(Integer id, String name, String description, String type) {
			this.id = id;
			this.name = name;
			this.description = description;
			this.type = type;
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

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
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
			Flour flour = (Flour) o;
			return Objects.equals( name, flour.name ) &&
					Objects.equals( description, flour.description ) &&
					Objects.equals( type, flour.type );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, description, type );
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class HQLUpdateQueryTest extends BaseReactiveTest {

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Flour.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		test( context, completedFuture( openSession() )
				.thenCompose( s -> s.persist( spelt ) )
				.thenCompose( s -> s.persist( rye ) )
				.thenCompose( s -> s.persist( almond ) )
				.thenCompose( s -> s.flush() ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, completedFuture( openSession() )
				.thenCompose( s -> s.remove( spelt ) )
				.thenCompose( s -> s.remove( rye ) )
				.thenCompose( s -> s.remove( almond ) )
				.thenCompose( s -> s.flush() ) );
	}

	@Test
	public void testUpdateQuery(TestContext context) {
		String updatedDescription =  "Most rye breads use a mix of rye and wheat flours";
		test(
				context,
				completedFuture( openSession() )
						.thenApply( s -> s.createQuery( "UPDATE Flour SET description = '" + updatedDescription + "' WHERE id = " + rye.getId()  ) )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> context.assertEquals( 1, resultCount ))
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, rye.getId() ) )
						.thenAccept( result -> context.assertEquals( updatedDescription, result.getDescription() ) )
		);
	}

	@Test
	public void testUpdateQueryWithParameters(TestContext context) {
		String updatedDescription =  "Most rye breads use a mix of rye and wheat flours";
		test(
				context,
				completedFuture( openSession() )
						.thenApply( s -> s.createQuery( "UPDATE Flour SET description = :updatedDescription WHERE id = :id" )
								.setParameter("updatedDescription", updatedDescription)
								.setParameter("id", rye.getId()) )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> context.assertEquals( 1, resultCount ))
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, rye.getId() ) )
						.thenAccept( result -> context.assertEquals( updatedDescription, result.getDescription() ) )
		);
	}

	@Test
	public void testInsertQuery(TestContext context) {
		Flour chestnut = new Flour( 777, "Chetnut", "The orginal ingredient for polenta", "gluten-free" );
		String updatedDescription =  "Most rye breads use a mix of rye and wheat flours";
		StringBuilder insertQueryBuilder = new StringBuilder( "insert into Flour(id, name, description, type) select " );
		insertQueryBuilder.append( chestnut.getId() ).append( ", " );
		insertQueryBuilder.append( "'" ).append( chestnut.getName() ).append( "', " );
		insertQueryBuilder.append( "'" ).append( chestnut.getDescription() ).append( "', " );
		insertQueryBuilder.append( "'" ).append( chestnut.getType() ).append( "' " );
		insertQueryBuilder.append( " from Flour where id = " + rye.getId() );
		test(
				context,
				completedFuture( openSession() )
						.thenApply( s -> s.createQuery( insertQueryBuilder.toString() ) )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resultCount -> context.assertEquals( 1, resultCount ))
						// Check if it's really be inserted
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, chestnut.getId() ) )
						.thenAccept( result -> context.assertEquals( chestnut, result ) )
						// Cleanup db
						.thenApply( v -> openSession() )
						.thenAccept( s -> s.remove( chestnut ) )
		);
	}

	@Test
	public void testDeleteQuery(TestContext context) {
		test(
				context,
				completedFuture( openSession() )
						.thenApply( s -> s.createQuery( "DELETE FROM Flour WHERE id = " + rye.getId() ) )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.executeUpdate();
						} )
						.thenAccept( resoultCount -> context.assertEquals( 1, resoultCount ) )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find( Flour.class, rye.getId() ) )
						.thenAccept( result -> context.assertNull( result ) )
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

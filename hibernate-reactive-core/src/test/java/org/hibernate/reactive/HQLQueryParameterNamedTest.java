/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Objects;

/**
 * Tests queries using named parameters like ":name",
 * as defined by the JPA specification.
 */
public class HQLQueryParameterNamedTest extends BaseReactiveTest {

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
		test( context, openSession()
				.thenCompose( s -> s.persist( spelt ) )
				.thenCompose( s -> s.persist( rye ) )
				.thenCompose( s -> s.persist( almond ) )
				.thenCompose( s -> s.flush() ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.remove( spelt ) )
				.thenCompose( s -> s.remove( rye ) )
				.thenCompose( s -> s.remove( almond ) )
				.thenCompose( s -> s.flush() ) );
	}

	@Test
	public void testAutoFlushOnSingleResult(TestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( semolina ) )
						.thenCompose( s ->
								s.createQuery( "from Flour where id = :id" )
										.setParameter( "id", semolina.getId())
										.getSingleResult()
										.thenAccept( found -> context.assertEquals( semolina, found ) )
										.thenCompose( v -> s.remove( semolina ) )
										.thenAccept( ss -> ss.flush() )
						)
		);
	}

	@Test
	public void testSelectScalarValues(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s ->
								s.createQuery( "SELECT 'Prova' FROM Flour WHERE id = :id" )
										.setParameter( "id", rye.getId() ) )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( found -> context.assertEquals( "Prova", found ) )
		);
	}

	@Test
	public void testSelectWithMultipleScalarValues(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s ->
								s.createQuery( "SELECT 'Prova', f.id FROM Flour f WHERE f.id = :id" )
										.setParameter("id", rye.getId() ))
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( found -> {
							context.assertTrue( found instanceof Object[] );
							context.assertEquals( "Prova", ( (Object[]) found )[0] );
							context.assertEquals( rye.getId(), ( (Object[]) found )[1] );
						} )
		);
	}

	@Test
	public void testSingleResultQueryOnId(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery( "FROM Flour WHERE id = :id" ).setParameter( "id", 1) )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( flour -> context.assertEquals( spelt, flour ) )
		);
	}

	@Test
	public void testSingleResultQueryOnName(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery( "FROM Flour WHERE name = :name" ).setParameter( "name", "Almond") )
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParameters(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery( "FROM Flour WHERE name = :name and description = :desc" )
								.setParameter( "name", almond.getName() )
								.setParameter( "desc", almond.getDescription() )
						)
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParametersReversed(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery( "FROM Flour WHERE name = :name and description = :desc" )
								.setParameter( "desc", almond.getDescription() )
								.setParameter( "name", almond.getName() )
						)
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParametersReused(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery( "FROM Flour WHERE name = :name or cast(:name as string) is null" )
								.setParameter( "name", almond.getName() )
						)
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testPlaceHolderInString(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery( "select ':', ':name', f FROM Flour f WHERE f.name = :name" )
								.setParameter( "name", almond.getName() )
						)
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( result -> {
							context.assertEquals( Object[].class, result.getClass() );
							final Object[] objects = (Object[]) result;
							context.assertEquals( 3, objects.length );
							context.assertEquals( ":", objects[0] );
							context.assertEquals( ":name", objects[1] );
							context.assertEquals( almond, objects[2] );
						})
		);
	}

	@Test
	public void testPlaceHolderAndSingleQuoteInString(TestContext context) {
		test(
				context,
				openSession()
						.thenApply( s -> s.createQuery("select ''':', ''':name''', f FROM Flour f WHERE f.name = :name")
								.setParameter( "name", almond.getName() )
						)
						.thenCompose( qr -> {
							context.assertNotNull( qr );
							return qr.getSingleResult();
						} )
						.thenAccept( result -> {
							context.assertEquals( Object[].class, result.getClass() );
							final Object[] objects = (Object[]) result;
							context.assertEquals( 3, objects.length );
							context.assertEquals( "':", objects[0] );
							context.assertEquals( "':name'", objects[1] );
							context.assertEquals( almond, objects[2] );
						})
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

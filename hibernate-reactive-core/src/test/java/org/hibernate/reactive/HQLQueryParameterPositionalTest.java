/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.reactive.stage.Stage;

import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;


/**
 * Tests queries using positional parameters like "?1, ?2, ...",
 * as defined by the JPA specification.
 *
 * Note that ORM treats such parameters as "named", not "positional";
 * that should be considered an internal implementation detail.
 */
public class HQLQueryParameterPositionalTest extends BaseReactiveTest {

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class );
	}

	@Before
	public void populateDb(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persistAll( spelt, rye, almond ) ) );
	}

	@Test
	public void testAutoFlushOnSingleResult(TestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withSession( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createQuery( "from Flour where id = ?1" )
						.setParameter( 1, semolina.getId() )
						.getSingleResult() )
				.thenAccept( found -> context.assertEquals( semolina, found ) )
		) );
	}

	@Test
	public void testSelectScalarValues(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "SELECT 'Prova' FROM Flour WHERE id = ?1" )
						  .setParameter( 1, rye.getId() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( found -> context.assertEquals( "Prova", found ) )
		);
	}

	@Test
	public void testSelectWithMultipleScalarValues(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "SELECT 'Prova', f.id FROM Flour f WHERE f.id = ?1" )
						  .setParameter( 1, rye.getId() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( found -> {
				  context.assertTrue( found instanceof Object[] );
				  context.assertEquals( "Prova", ( (Object[]) found )[0] );
				  context.assertEquals( rye.getId(), ( (Object[]) found )[1] );
			  } )
		);
	}

	@Test
	public void testSingleResultQueryOnId(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE id = ?1" ).setParameter( 1, 1);
				context.assertNotNull( qr );
				return qr.getSingleResult();
			} ).thenAccept( flour -> context.assertEquals( spelt, flour ) )
		);
	}

	@Test
	public void testSingleResultQueryOnName(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = ?1" ).setParameter( 1, "Almond" );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParameters(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = ?1 and description = ?2" )
						  .setParameter( 1, almond.getName() )
						  .setParameter( 2, almond.getDescription() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParametersReversed(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = ?2 and description = ?1" )
						  .setParameter( 2, almond.getName() )
						  .setParameter( 1, almond.getDescription() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParametersReused(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = ?1 or cast(?1 as string) is null" )
						  .setParameter( 1, almond.getName() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testPlaceHolderInString(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "select '?', '?1', f FROM Flour f WHERE f.name = ?1" )
						  .setParameter( 1, almond.getName() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( result -> {
				  context.assertEquals( Object[].class, result.getClass() );
				  final Object[] objects = (Object[]) result;
				  context.assertEquals( 3, objects.length );
				  context.assertEquals( "?", objects[0] );
				  context.assertEquals( "?1", objects[1] );
				  context.assertEquals( almond, objects[2] );
			  } )
		);
	}

	@Test
	public void testPlaceHolderAndSingleQuoteInString(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "select '''?', '''?1''', f FROM Flour f WHERE f.name = ?1" )
						  .setParameter( 1, almond.getName() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( result -> {
				  context.assertEquals( Object[].class, result.getClass() );
				  final Object[] objects = (Object[]) result;
				  context.assertEquals( 3, objects.length );
				  context.assertEquals( "'?", objects[0] );
				  context.assertEquals( "'?1'", objects[1] );
				  context.assertEquals( almond, objects[2] );
			  } )
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

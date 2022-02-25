/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;


public class HQLQueryTest extends BaseReactiveTest {

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
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persistAll( spelt, rye, almond ) ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.createQuery( "delete Flour" ).executeUpdate() ) );
	}

	@Test
	public void testAutoFlushOnSingleResult(TestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withSession( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createQuery( "from Flour where id = " + semolina.getId() ).getSingleResult() )
				.thenAccept( found -> context.assertEquals( semolina, found ) ) )
		);
	}

	@Test
	public void testAutoFlushOnResultList(TestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withSession( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createQuery( "from Flour order by name" ).getResultList()
						.thenAccept( results -> {
							context.assertNotNull( results );
							context.assertEquals( 4, results.size() );
							context.assertEquals( almond, results.get( 0 ) );
							context.assertEquals( rye, results.get( 1 ) );
							context.assertEquals( semolina, results.get( 2 ) );
							context.assertEquals( spelt, results.get( 3 ) );
						} )
				)
		) );
	}

	@Test
	public void testSelectScalarValues(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "SELECT 'Prova' FROM Flour WHERE id = " + rye.getId() );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( found -> context.assertEquals( "Prova", found ) )
		);
		test( context, getSessionFactory().withSession( s -> {
					Stage.Query<Long> qr = s.createQuery( "SELECT count(*) FROM Flour", Long.class );
					context.assertNotNull( qr );
					return qr.getSingleResult();
				} ).thenAccept( found -> context.assertEquals(3L, found ) )
		);
	}

	@Test
	public void testSelectWithMultipleScalarValues(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<?> qr = s.createQuery( "SELECT 'Prova', f.id FROM Flour f WHERE f.id = " + rye.getId() );
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
				  Stage.Query<?> qr = s.createQuery( "FROM Flour WHERE id = 1" );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> context.assertEquals( spelt, flour ) )
		);
	}

	@Test
	public void testSingleResultQueryOnName(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<?> qr = s.createQuery( "FROM Flour WHERE name = 'Almond'" );
				  context.assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> context.assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testFromQuery(TestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<?> qr = s.createQuery( "FROM Flour ORDER BY name" ) ;
				  context.assertNotNull( qr );
				  return qr.getResultList();
			  } ).thenAccept( flours -> {
				  context.assertNotNull( flours );
				  context.assertEquals( 3, flours.size() );
				  context.assertEquals( almond, flours.get( 0 ) );
				  context.assertEquals( rye, flours.get( 1 ) );
				  context.assertEquals( spelt, flours.get( 2 ) );
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

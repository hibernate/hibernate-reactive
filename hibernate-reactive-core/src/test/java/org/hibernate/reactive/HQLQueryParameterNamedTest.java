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

import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests queries using named parameters like ":name",
 * as defined by the JPA specification.
 */
public class HQLQueryParameterNamedTest extends BaseReactiveTest {

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class );
	}

	public CompletionStage<Void> populateDb() {
		return openSession().thenCompose( s -> s.persist( spelt, rye, almond).thenCompose( v -> s.flush() ) );
	}

	@Test
	public void testAutoFlushOnSingleResult(VertxTestContext context) {
		Flour semolina = new Flour(678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, populateDb().thenCompose( vd -> getSessionFactory()
				.withSession( s -> s
						.persist( semolina )
						.thenCompose( v -> s.createQuery( "from Flour where id = :id" )
								.setParameter( "id", semolina.getId() )
								.getSingleResult()
						)
						.thenAccept( found -> assertEquals( semolina, found ) )
				) )
		);
	}

	@Test
	public void testSelectScalarValues(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "SELECT 'Prova' FROM Flour WHERE id = :id" )
						  .setParameter( "id", rye.getId() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( found -> assertEquals( "Prova", found ) )
		);
	}

	@Test
	public void testSelectWithMultipleScalarValues(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "SELECT 'Prova', f.id FROM Flour f WHERE f.id = :id" )
						  .setParameter( "id", rye.getId() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( found -> {
				  assertTrue( found instanceof Object[] );
				  assertEquals( "Prova", ( (Object[]) found )[0] );
				  assertEquals( rye.getId(), ( (Object[]) found )[1] );
			  } )
		);
	}

	@Test
	public void testSingleResultQueryOnId(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE id = :id" )
						  .setParameter( "id", 1 );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( flour -> assertEquals( spelt, flour ) )
		);
	}

	@Test
	public void testSingleResultQueryOnName(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = :name" )
						  .setParameter( "name", "Almond" );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( flour -> assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParameters(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = :name and description = :desc" )
						  .setParameter( "name", almond.getName() )
						  .setParameter( "desc", almond.getDescription() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( flour -> assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParametersReversed(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = :name and description = :desc" )
						  .setParameter( "desc", almond.getDescription() )
						  .setParameter( "name", almond.getName() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( flour -> assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testSingleResultMultipleParametersReused(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "FROM Flour WHERE name = :name or cast(:name as string) is null" )
						  .setParameter( "name", almond.getName() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( flour -> assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testPlaceHolderInString(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "select ':', ':name', f FROM Flour f WHERE f.name = :name" )
						  .setParameter( "name", almond.getName() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( result -> {
				  assertEquals( Object[].class, result.getClass() );
				  final Object[] objects = (Object[]) result;
				  assertEquals( 3, objects.length );
				  assertEquals( ":", objects[0] );
				  assertEquals( ":name", objects[1] );
				  assertEquals( almond, objects[2] );
			  } )
		);
	}

	@Test
	public void testPlaceHolderAndSingleQuoteInString(VertxTestContext context) {
		test( context, populateDb().thenCompose( vd -> getSessionFactory().withSession( s -> {
				  Stage.Query<Object> qr = s.createQuery( "select ''':', ''':name''', f FROM Flour f WHERE f.name = :name" )
						  .setParameter( "name", almond.getName() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ) ).thenAccept( result -> {
				  assertEquals( Object[].class, result.getClass() );
				  final Object[] objects = (Object[]) result;
				  assertEquals( 3, objects.length );
				  assertEquals( "':", objects[0] );
				  assertEquals( "':name'", objects[1] );
				  assertEquals( almond, objects[2] );
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

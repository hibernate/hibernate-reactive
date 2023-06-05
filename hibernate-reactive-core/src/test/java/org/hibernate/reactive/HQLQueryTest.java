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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class HQLQueryTest extends BaseReactiveTest {

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persistAll( spelt, rye, almond ) ) );
	}

	@Test
	public void testAutoFlushOnSingleResult(VertxTestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withTransaction( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createQuery( "from Flour where id = " + semolina.getId() ).getSingleResult() )
				.thenAccept( found -> assertEquals( semolina, found ) ) )
		);
	}

	@Test
	public void testAutoFlushOnResultList(VertxTestContext context) {
		Flour semolina = new Flour( 678, "Semoline", "the coarse, purified wheat middlings of durum wheat used in making pasta.", "Wheat flour" );
		test( context, getSessionFactory().withTransaction( s -> s
				.persist( semolina )
				.thenCompose( v -> s.createQuery( "from Flour order by name" ).getResultList() )
				.thenAccept( results -> assertThat( results ).containsExactly( almond, rye, semolina, spelt ) )
		) );
	}

	@Test
	public void testSelectScalarString(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			Stage.Query<Object> qr = s.createQuery( "SELECT 'Prova' FROM Flour WHERE id = " + rye.getId() );
			assertNotNull( qr );
			return qr.getSingleResult();
		} ).thenAccept( found -> assertEquals( "Prova", found ) ) );
	}

	@Test
	public void testSelectScalarCount(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			Stage.SelectionQuery<Long> qr = s.createQuery( "SELECT count(*) FROM Flour", Long.class );
			assertNotNull( qr );
			return qr.getSingleResult();
		} ).thenAccept( found -> assertEquals( 3L, found ) ) );
	}

	@Test
	public void testSelectWithMultipleScalarValues(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<?> qr = s.createQuery( "SELECT 'Prova', f.id FROM Flour f WHERE f.id = " + rye.getId() );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( found -> {
				  assertTrue( found instanceof Object[] );
				  assertEquals( "Prova", ( (Object[]) found )[0] );
				  assertEquals( rye.getId(), ( (Object[]) found )[1] );
			  } )
		);
	}

	@Test
	public void testSingleResultQueryOnId(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<?> qr = s.createQuery( "FROM Flour WHERE id = 1" );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> assertEquals( spelt, flour ) )
		);
	}

	@Test
	public void testSingleResultQueryOnName(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
				  Stage.Query<?> qr = s.createQuery( "FROM Flour WHERE name = 'Almond'" );
				  assertNotNull( qr );
				  return qr.getSingleResult();
			  } ).thenAccept( flour -> assertEquals( almond, flour ) )
		);
	}

	@Test
	public void testFromQuery(VertxTestContext context) {
		test( context, getSessionFactory()
				.withSession( s -> {
					Stage.Query<Flour> qr = s.createQuery( "FROM Flour ORDER BY name" );
					assertNotNull( qr );
					return qr.getResultList();
				} )
				.thenAccept( results -> assertThat( results ).containsExactly( almond, rye, spelt ) )
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

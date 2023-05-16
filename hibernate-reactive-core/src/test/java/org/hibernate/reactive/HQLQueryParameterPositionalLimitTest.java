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

import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Tests queries using positional parameters like "?1, ?2, ...",
 * as defined by the JPA specification, along with limit parameters.
 *
 * Note that ORM treats such parameters as "named", not "positional";
 * that should be considered an internal implementation detail.
 */
public class HQLQueryParameterPositionalLimitTest extends BaseReactiveTest {

	// Db2: Parameter at position[0] with class = [java.lang.Integer] and value = [1] can not be coerced to the expected class = [java.lang.Double] for encoding.
	@RegisterExtension
	public DBSelectionExtension skip = skipTestsFor( DB2 );

	Flour spelt = new Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class );
	}

//	@BeforeEach
//	public void populateDb(VertxTestContext context) {
//		test( context,openSession()
//				.thenCompose( s -> s.persist( spelt )
//				.thenCompose( v -> s.persist( rye ) )
//				.thenCompose( v -> s.persist( almond ) )
//				.thenCompose( v -> s.flush() ) )
//		);
//	}

	public CompletionStage<Void> populateDb() {
		return getSessionFactory().withTransaction( s -> s.persist( spelt, rye, almond ) );
	}

	@Test
	public void testNoResults(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour where id = ?1" ).setMaxResults( 0 )
										.setParameter( 1, rye.getId() )
										.getResultList()
										.thenAccept( list -> assertEquals( 0, list.size() ) )
						) )
		);
	}

	@Test
	public void testFirstResultNoResults(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour" )
										.setMaxResults( 0 )
										.setFirstResult( 1 )
										.getResultList()
										.thenAccept( list -> assertEquals( 0, list.size() ) )
						) )
		);
	}

	@Test
	public void testFirstResultSingleResult(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour where name != ?1 order by id" )
										.setParameter( 1, spelt.getName() )
										.setFirstResult( 1 )
										.getSingleResult()
										.thenAccept( result -> assertEquals( almond, result ) )
						) )
		);
	}

	@Test
	public void testFirstResultMultipleResults(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour order by id" )
										.setFirstResult( 1 )
										.getResultList()
										.thenAccept( results -> {
											assertEquals( 2, results.size() );
											assertEquals( rye, results.get( 0 ) );
											assertEquals( almond, results.get( 1 ) );
										} )
						) )
		);
	}

	@Test
	public void testFirstResultMaxResultsSingleResult(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour order by id" )
										.setFirstResult( 1 )
										.setMaxResults( 1 )
										.getSingleResult()
										.thenAccept( result -> {
											assertEquals( rye, result );
										} )
						) )
		);
	}

	/**
	 * Some databases (see MSSQL) generate a different SQL query when the first result is set to 0
	 */
	@Test
	public void testFirstResultZeroAndMaxResults(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
											  s.createQuery( "from Flour where name = ?1 order by id" )
													  .setParameter( 1, almond.getName() )
													  .setFirstResult( 0 )
													  .setMaxResults( 10 )
													  .getResultList()
													  .thenAccept( results -> {
														  assertEquals( 1, results.size() );
														  assertEquals( almond, results.get( 0 ) );
													  } )
						) )
		);
	}

	/**
	 * Some databases (see MSSQL) generate a different SQL query to limit the results
	 * when the HQL query has no order by clause
	 */
	@Test
	public void testFirstResultZeroAndMaxResultsWithoutOrder(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
											  s.createQuery( "from Flour where name = ?1" )
													  .setParameter( 1, almond.getName() )
													  .setFirstResult( 0 )
													  .setMaxResults( 10 )
													  .getResultList()
													  .thenAccept( results -> {
														  assertEquals( 1, results.size() );
														  assertEquals( almond, results.get( 0 ) );
													  } )
						) )
		);
	}

	@Test
	public void testFirstResultMaxResultsMultipleResults(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour order by id" )
										.setFirstResult( 1 )
										.setMaxResults( 2 )
										.getResultList()
										.thenAccept( results -> {
											assertEquals( 2, results.size() );
											assertEquals( rye, results.get( 0 ) );
											assertEquals( almond, results.get( 1 ) );
										} )
						) )
		);
	}

	@Test
	public void testFirstResultMaxResultsExtra(VertxTestContext context) {
		test(
				context,
				populateDb().thenCompose( vd -> openSession()
						.thenCompose( s ->
								s.createQuery( "from Flour order by id" )
										.setFirstResult( 1 )
										.setMaxResults( 3 )
										.getResultList()
										.thenAccept( results -> {
											assertEquals( 2, results.size() );
											assertEquals( rye, results.get( 0 ) );
											assertEquals( almond, results.get( 1 ) );
										} )
						) )
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

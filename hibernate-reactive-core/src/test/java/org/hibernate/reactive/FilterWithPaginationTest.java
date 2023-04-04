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
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.type.descriptor.java.StringJavaType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.skipTestsFor;

/**
 * Test the combination of filters, max results and first result.
 */
public class FilterWithPaginationTest extends BaseReactiveTest {

	// Db2: java.lang.ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Long
	@Rule
	public final DatabaseSelectionRule skipDb2 = skipTestsFor( DB2 );

	FamousPerson margaret = new FamousPerson( 1L, "Margaret Howe Lovatt", Status.LIVING, "the woman who lived in a half-flooded home with a dolphin." );
	FamousPerson nellie = new FamousPerson( 2L, "Nellie Bly", Status.DECEASED, "In 1888, she traveled around the world in 72 days." );
	FamousPerson hedy = new FamousPerson( 3L, "Hedy Lamarr", Status.DECEASED, "Actress and co-inventor of an early version of frequency-hopping spread spectrum communication for torpedo guidance." );

	// I need at least a couple with the same name to test parameterized filters
	FamousPerson rebeccaActress = new FamousPerson( 4L, "Rebecca Ferguson", Status.LIVING, "Known for her Golden Globe-nominated performance in BBC series The White Queen." );
	FamousPerson rebeccaSinger = new FamousPerson( 5L, "Rebecca Ferguson", Status.LIVING, "Shot to fame on The X Factor and is currently a solo artist." );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( FamousPerson.class );
	}

	@Before
	public void populateDb(TestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> s
					  .persistAll( margaret, nellie, hedy, rebeccaActress, rebeccaSinger )
					  .chain( s::flush ) ) );
	}

	/**
	 * Sql server run different queries based if order-by is missing and there are no filters
	 */
	@Test
	public void testMaxResultsAndOffsetWithStageWithBasicQuery(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_BASIC_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				// I cannot use the order-by, so I'm not sure which result it will return
				.thenAccept( list -> {
					assertThat( list ).containsAnyOf( margaret, nellie, hedy, rebeccaActress, rebeccaSinger );
					assertThat( list ).hasSize( 2 );
				} )
		);
	}

	@Test
	public void testOffsetWithStageWithBasicQuery(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_BASIC_QUERY )
						.setFirstResult( 3 )
						.getResultList() )
				// I cannot use the order-by, so I'm not sure which results it will return
				.thenAccept( list -> {
					assertThat( list ).containsAnyOf( margaret, nellie, hedy, rebeccaActress, rebeccaSinger );
					assertThat( list ).hasSize( 2 );
				} )
		);
	}

	@Test
	public void testMaxResultsWithStageWithBasicQuery(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_BASIC_QUERY )
						.setMaxResults( 4 )
						.getResultList() )
				// I cannot use the order-by, so I'm not sure which result it will return
				.thenAccept( list -> {
					assertThat( list ).containsAnyOf( margaret, nellie, hedy, rebeccaActress, rebeccaSinger );
					assertThat( list ).hasSize( 4 );
				} )
		);
	}

	@Test
	public void testMaxResultsWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testMaxResultsWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testFirstResultWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 1 )
						.getSingleResult() )
				.thenAccept( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 1 )
						.getSingleResult() )
				.invoke( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList()
						.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithStage(TestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithMutiny(TestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	private static CompletionStage<Stage.Session> enableFilter(
			CompletionStage<Stage.Session> stageSession,
			String filterName,
			Object... parameters) {
		return stageSession.thenApply( session -> {
			org.hibernate.Filter filter = session.enableFilter( filterName );
			for ( int j = 0; j < parameters.length; j++ ) {
				String paramName = (String) parameters[j++];
				Object paramValue = parameters[j++];
				filter.setParameter( paramName, paramValue );
			}
			return session;
		} );
	}

	private static Uni<Mutiny.Session> enableFilter(
			Uni<Mutiny.Session> uniSession,
			String filterName,
			Object... parameters) {
		return uniSession.map( session -> {
			org.hibernate.Filter filter = session.enableFilter( filterName );
			for ( int j = 0; j < parameters.length; j++ ) {
				String paramName = (String) parameters[j++];
				Object paramValue = parameters[j++];
				filter.setParameter( paramName, paramValue );
			}
			return session;
		} );
	}

	public enum Status {
		LIVING,
		DECEASED
	}

	@Entity(name = "FamousPerson")
	// No order by, No filters: Sql server changes the query based on these
	@NamedQuery(name = FamousPerson.FIND_ALL_BASIC_QUERY, query = "from FamousPerson p")
	@NamedQuery(name = FamousPerson.FIND_ALL_QUERY, query = "from FamousPerson p order by p.id")
	@FilterDef(name = FamousPerson.HAS_NAME_FILTER, defaultCondition = "name = :name", parameters = @ParamDef(name = "name", type = StringJavaType.class))
	@FilterDef(name = FamousPerson.IS_ALIVE_FILTER, defaultCondition = "status = 'LIVING'")
	@Filter(name = FamousPerson.IS_ALIVE_FILTER)
	@Filter(name = FamousPerson.HAS_NAME_FILTER)
	public static class FamousPerson {

		static final String FIND_ALL_BASIC_QUERY = "Person.basicFindAll";

		static final String FIND_ALL_QUERY = "Person.findAll";
		static final String IS_ALIVE_FILTER = "Person.isAlive";
		static final String HAS_NAME_FILTER = "Person.hasName";

		@Id
		public Long id;

		public String name;

		public String description;

		@Enumerated(EnumType.STRING)
		public Status status;

		public FamousPerson() {
		}

		public FamousPerson(Long id, String name, Status status, String description) {
			this.id = id;
			this.name = name;
			this.status = status;
			this.description = description;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			FamousPerson person = (FamousPerson) o;
			return Objects.equals( name, person.name ) && Objects.equals(
					description,
					person.description
			) && status == person.status;
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, description, status );
		}

		@Override
		public String toString() {
			return id + ":" + name + " is " + status;
		}
	}
}

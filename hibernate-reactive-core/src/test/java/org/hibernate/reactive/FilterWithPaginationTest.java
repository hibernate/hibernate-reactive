/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.type.descriptor.java.StringJavaType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.query.Page.first;
import static org.hibernate.query.Page.page;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

/**
 * Test the combination of filters, max results, first result, and {@link org.hibernate.query.Page}.
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "IllegalStateException: Needed to have 6 in buffer but only had 0")
public class FilterWithPaginationTest extends BaseReactiveTest {

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

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> s
				.persistAll( margaret, nellie, hedy, rebeccaActress, rebeccaSinger )
				.chain( s::flush ) ) );
	}

	/**
	 * Sql server run different queries based if order-by is missing and there are no filters
	 */
	@Test
	public void testMaxResultsAndOffsetWithStageWithBasicQuery(VertxTestContext context) {
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

	/**
	 * Sql server run different queries based if order-by is missing and there are no filters
	 */
	@Test
	public void testMaxResultsAndOffsetWithStageWithBasicQueryAndPage(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_BASIC_QUERY )
						.setPage( page( 2, 0 ) )
						.getResultList() )
				// I cannot use the order-by, so I'm not sure which result it will return
				.thenAccept( list -> {
					assertThat( list ).containsAnyOf( margaret, nellie, hedy, rebeccaActress, rebeccaSinger );
					assertThat( list ).hasSize( 2 );
				} )
		);
	}

	@Test
	public void testOffsetWithStageWithBasicQuery(VertxTestContext context) {
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
	public void testWithStageWithBasicQueryAndPage(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_BASIC_QUERY )
						.setPage( first( 3 ) )
						.getResultList() )
				// I cannot use the order-by, so I'm not sure which results it will return
				.thenAccept( list -> {
					assertThat( list ).containsAnyOf( margaret, nellie, hedy, rebeccaActress, rebeccaSinger );
					assertThat( list ).hasSize( 3 );
				} )
		);
	}

	@Test
	public void testMaxResultsWithStageWithBasicQuery(VertxTestContext context) {
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
	public void testMaxResultsWithStageWithBasicQueryAndPage(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_BASIC_QUERY )
						.setPage( first( 4 ) )
						.getResultList() )
				// I cannot use the order-by, so I'm not sure which result it will return
				.thenAccept( list -> {
					assertThat( list ).containsAnyOf( margaret, nellie, hedy, rebeccaActress, rebeccaSinger );
					assertThat( list ).hasSize( 4 );
				} )
		);
	}

	@Test
	public void testMaxResultsWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testMaxResultsWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 2 ) )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testMaxResultsWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testMaxResultsWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 2 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testFirstResultWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 2, 1 ) )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 2, 1 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 2, 1 ) )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 2, 1 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 2 ) )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 2 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 1 )
						.getSingleResult() )
				.thenAccept( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 1 ) )
						.getSingleResult() )
				.thenAccept( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 1 )
						.getSingleResult() )
				.invoke( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 1 ) )
						.getSingleResult() )
				.invoke( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList()
						.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 1, 1 ) )
						.getResultList()
						.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 1, 1 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithStage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithStageAndPage(VertxTestContext context) {
		test( context, enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.thenCompose( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 2 ) )
						.getResultList() )
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithMutiny(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setMaxResults( 2 )
						.setFirstResult( 1 )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithMutinyAndPage(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( page( 1, 1 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}


	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithMutinyAndPageFirst(VertxTestContext context) {
		test( context, enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name )
				.chain( session -> session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
						.setPage( first( 2 ) )
						.getResultList() )
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	private static CompletionStage<Stage.Session> enableFilter(
			CompletionStage<Stage.Session> stageSession,
			String filterName,
			// An array of [paramName, paramValue, paramName, paramValue, ...]
			Object... parameters) {
		return stageSession.thenApply( session -> {
			org.hibernate.Filter filter = session.enableFilter( filterName );
			for ( int j = 0; j < parameters.length; j += 2 ) {
				String paramName = (String) parameters[j];
				Object paramValue = parameters[j + 1];
				filter.setParameter( paramName, paramValue );
			}
			return session;
		} );
	}

	private static Uni<Mutiny.Session> enableFilter(
			Uni<Mutiny.Session> uniSession,
			String filterName,
			// An array of [paramName, paramValue, paramName, paramValue, ...]
			Object... parameters) {
		return uniSession.map( session -> {
			org.hibernate.Filter filter = session.enableFilter( filterName );
			for ( int j = 0; j < parameters.length; j += 2 ) {
				String paramName = (String) parameters[j];
				Object paramValue = parameters[j+1];
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

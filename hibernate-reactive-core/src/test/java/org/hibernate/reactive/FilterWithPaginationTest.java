/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.NamedQuery;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the combination of filters, max results and first result.
 */
public class FilterWithPaginationTest extends BaseReactiveTest {

	/**
	 * There's probably a bug in the Vert.x Db2 SQL client.
	 * See: https://github.com/eclipse-vertx/vertx-sql-client/issues/887
	 */
	@Rule
	public DatabaseSelectionRule skipDB2 = DatabaseSelectionRule.skipTestsFor( DatabaseConfiguration.DBType.DB2 );

	FamousPerson margaret = new FamousPerson( 1L, "Margaret Howe Lovatt", Status.LIVING, "the woman who lived in a half-flooded home with a dolphin.");
	FamousPerson nellie = new FamousPerson( 2L, "Nellie Bly", Status.DECEASED, "In 1888, she traveled around the world in 72 days.");
	FamousPerson hedy = new FamousPerson( 3L, "Hedy Lamarr", Status.DECEASED, "Actress and co-inventor of an early version of frequency-hopping spread spectrum communication for torpedo guidance.");

	// I need at least a couple with the same name to test parameterized filters
	FamousPerson rebeccaActress = new FamousPerson( 4L, "Rebecca Ferguson", Status.LIVING, "Known for her Golden Globe-nominated performance in BBC series The White Queen.");
	FamousPerson rebeccaSinger = new FamousPerson( 5L, "Rebecca Ferguson", Status.LIVING, "Shot to fame on The X Factor and is currently a solo artist.");

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( FamousPerson.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> s
				.persistAll( margaret, nellie, hedy, rebeccaActress, rebeccaSinger )
				.chain( s::flush )
			  )
		);
	}

	@After
	public void clearDb(TestContext context) {
		test( context, getMutinySessionFactory().withTransaction( (s, tx) -> s
				.createQuery( "delete from FamousPerson" ).executeUpdate() )
		);
	}

	@Test
	public void testMaxResultsWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testMaxResultsWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testFirstResultWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setFirstResult( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setFirstResult( 1 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.setFirstResult( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndOffsetWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.setFirstResult( 1 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsForParameterizedFilterWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 1 )
				.getSingleResult()
				.thenAccept( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testSingleResultMaxResultsForParameterizedFilterWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 1 )
				.getSingleResult()
				.invoke( result -> assertThat( result ).isEqualTo( rebeccaActress ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setFirstResult( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testFirstResultForParameterizedFilterWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setFirstResult( 1 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.setFirstResult( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testMaxResultsAndFirstResultForParameterizedFilterWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), FamousPerson.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( FamousPerson.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.setFirstResult( 1 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	private static Stage.Session enableFilter(Stage.Session session, String filterName, Object... parameters) {
		org.hibernate.Filter filter = session.enableFilter( filterName );
		for ( int j = 0; j < parameters.length; j++ ) {
			String paramName = (String) parameters[j++];
			Object paramValue = parameters[j++];
			filter.setParameter( paramName, paramValue );
		}
		return session;
	}

	private static Mutiny.Session enableFilter(Mutiny.Session session, String filterName, Object... parameters) {
		org.hibernate.Filter filter = session.enableFilter( filterName );
		for ( int j = 0; j < parameters.length; j++ ) {
			String paramName = (String) parameters[j++];
			Object paramValue = parameters[j++];
			filter.setParameter( paramName, paramValue );
		}
		return session;
	}

	public enum Status {
		LIVING,
		DECEASED
	}

	@Entity(name = "FamousPerson")
	@NamedQuery(name = FamousPerson.FIND_ALL_QUERY, query = "from FamousPerson p order by p.id")
	@FilterDef(name = FamousPerson.HAS_NAME_FILTER, defaultCondition = "name = :name", parameters = @ParamDef(name = "name", type = "string"))
	@FilterDef(name = FamousPerson.IS_ALIVE_FILTER, defaultCondition = "status = 'LIVING'")
	@Filter(name = FamousPerson.IS_ALIVE_FILTER)
	@Filter(name = FamousPerson.HAS_NAME_FILTER)
	public static class FamousPerson {

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

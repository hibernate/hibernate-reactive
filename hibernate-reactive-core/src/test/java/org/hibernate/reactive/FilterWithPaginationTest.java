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
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the combination of filters, max results and first result.
 */
public class FilterWithPaginationTest extends BaseReactiveTest {

	Person margaret = new Person(1L, "Margaret Howe Lovatt",Status.LIVING, "the woman who lived in a half-flooded home with a dolphin.");
	Person nellie = new Person(2L, "Nellie Bly", Status.DECEASED, "In 1888, she traveled around the world in 72 days.");
	Person hedy = new Person(3L, "Hedy Lamarr", Status.DECEASED, "Actress and co-inventor of an early version of frequency-hopping spread spectrum communication for torpedo guidance.");

	// I need at least a couple with the same name to test parameterized filters
	Person rebeccaActress = new Person(4L, "Rebecca Ferguson", Status.LIVING, "Known for her Golden Globe-nominated performance in BBC series The White Queen.");
	Person rebeccaSinger = new Person( 5L, "Rebecca Ferguson", Status.LIVING, "Shot to fame on The X Factor and is currently a solo artist.");

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		test( context, getMutinySessionFactory().withTransaction( (s, tx) -> s
				.persistAll( margaret, nellie, hedy, rebeccaActress, rebeccaSinger ) ) );
	}

	@After
	public void clearDb(TestContext context) {
		test( context, getMutinySessionFactory().withTransaction( (s, tx) -> s
				.createNativeQuery( "delete from Person" ).executeUpdate() ) );
	}

	@Test
	public void testFilterWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), Person.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testFilterWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), Person.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( margaret, rebeccaActress ) )
		);
	}

	@Test
	public void testFilterAndOffsetWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), Person.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.setFirstResult( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testFilterAndOffsetWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), Person.IS_ALIVE_FILTER );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.setFirstResult( 1 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testParameterizedFilterWithMultipleResultsWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), Person.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testParameterizedFilterWithMultipleResultsWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), Person.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 2 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress, rebeccaSinger ) )
		);
	}

	@Test
	public void testParameterizedFilterWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), Person.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaActress ) )
		);
	}

	@Test
	public void testParameterizedFilterWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), Person.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 1 )
				.getResultList()
				.invoke( list -> assertThat( list ).containsExactly( rebeccaActress ) )
		);
	}

	@Test
	public void testParameterizedFilterAndOffsetWithStage(TestContext context) {
		Stage.Session session = enableFilter( openSession(), Person.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 1 )
				.setFirstResult( 1 )
				.getResultList()
				.thenAccept( list -> assertThat( list ).containsExactly( rebeccaSinger ) )
		);
	}

	@Test
	public void testParameterizedFilterAndOffsetWithMutiny(TestContext context) {
		Mutiny.Session session = enableFilter( openMutinySession(), Person.HAS_NAME_FILTER, "name", rebeccaActress.name );
		test( context, session.createNamedQuery( Person.FIND_ALL_QUERY )
				.setMaxResults( 1 )
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

	@Entity(name = "Person")
	@NamedQuery(name = Person.FIND_ALL_QUERY, query = "from Person order by id")
	@FilterDef(name = Person.HAS_NAME_FILTER, defaultCondition = "name = :name", parameters = @ParamDef(name = "name", type = "string"))
	@FilterDef(name = Person.IS_ALIVE_FILTER, defaultCondition = "status = 'LIVING'")
	@Filter(name = Person.IS_ALIVE_FILTER)
	@Filter(name = Person.HAS_NAME_FILTER)
	public static class Person {

		static final String FIND_ALL_QUERY = "Person.findAll";
		static final String IS_ALIVE_FILTER = "Person.isAlive";
		static final String HAS_NAME_FILTER = "Person.hasName";

		@Id
		public Long id;

		public String name;

		public String description;

		@Enumerated(EnumType.STRING)
		public Status status;

		public Person() {
		}

		public Person(Long id, String name, Status status, String description) {
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
			Person person = (Person) o;
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

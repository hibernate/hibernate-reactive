/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.annotations.SQLSelect;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.reactive.annotations.EnableFor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.SQLSelectTest.Person.SELECT_QUERY;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

@EnableFor(value = POSTGRESQL, reason = "Native queries for this test are targeted for PostgreSQL")
@EnableFor(value = COCKROACHDB, reason = "Native queries for this test are targeted for CockroachDB")
public class SQLSelectTest extends BaseReactiveTest {

	private SqlStatementTracker sqlTracker;

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( SQLSelectTest::doCheckQuery, configuration.getProperties() );
		return configuration;
	}

	private static boolean doCheckQuery(String s) {
		return s.toLowerCase().startsWith( "select" );
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		thePerson = new Person();
		thePerson.id = 724200;
		thePerson.name = "Claude";

		test( context, getMutinySessionFactory().withTransaction( s -> s.persist( thePerson ) ) );
	}

	@Test
	public void findEntity(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( thePerson );
					assertThat( sqlTracker.getLoggedQueries() )
							.containsExactly(
									"select version()",
									SELECT_QUERY.replace( "?", "$1" )
							);
				} )
		);
	}


	@Entity(name = "Person")
	@SQLSelect(sql = SELECT_QUERY)
	static class Person {
		// Query containing simple where check to distinguish from a possible generated query
		public static final String SELECT_QUERY = "SELECT id, name FROM person WHERE id = ? and 'hreact' = 'hreact'";

		@Id
		private int id;

		private String name;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
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
			return Objects.equals( name, person.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}

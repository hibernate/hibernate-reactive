/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


import org.hibernate.annotations.SQLSelect;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DBSelectionExtension.runOnlyFor;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SQLSelectTest extends BaseReactiveTest {
	@RegisterExtension // We use native queries, which may be different for other DBs
	public DBSelectionExtension dbSelection = runOnlyFor( POSTGRESQL );

	private SqlStatementTracker sqlTracker;
	private Person thePerson;
	public static String SQL = "xxxxx";

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
		List<String> phones = Arrays.asList( "999-999-9999", "111-111-1111", "123-456-7890" );
		thePerson = new Person();
		thePerson.id = 724200;
		thePerson.name = "Claude";
		thePerson.phones = phones;

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) ) );
	}

	@Test
	public void findEntity(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.find( Person.class, thePerson.getId() ) )
				.thenAccept( found -> {
					assertPhones( found, "999-999-9999", "111-111-1111", "123-456-7890" );
					assertThat( sqlTracker.getLoggedQueries() ).contains( Person.SELECT_QUERY.replace( "?", "$1" ) );
				} )
		);
	}

	private static void assertPhones(Person person, String... expectedPhones) {
		assertNotNull( person );
		assertThat( person.getPhones() ).containsExactlyInAnyOrder( expectedPhones );
	}

	@Entity(name = "Person")
	@SQLSelect(sql = Person.SELECT_QUERY)
	static class Person {
		// Query containing simple where check to distinguish from a possible generated query
		public static final String SELECT_QUERY = "SELECT id, name FROM person WHERE id = ? and 'hreact' = 'hreact'";

		@Id
		private int id;

		private String name;

		@ElementCollection(fetch = FetchType.EAGER)
		private List<String> phones = new ArrayList<>();

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

		public List<String> getPhones() {
			return phones;
		}
	}
}

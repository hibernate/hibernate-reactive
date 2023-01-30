/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * This test class verifies that data can be persisted and queried on the same database
 * using both JPA/hibernate and reactive session factories.
 */
public class ORMReactivePersistenceTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	private SessionFactory ormFactory;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Before
	public void prepareOrmFactory() {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( Settings.DRIVER, "org.postgresql.Driver" );
		configuration.setProperty( Settings.DIALECT, PostgreSQLDialect.class.getName() );

		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
				.applySettings( configuration.getProperties() );

		StandardServiceRegistry registry = builder.build();
		ormFactory = configuration.buildSessionFactory( registry );
	}

	@After
	public void closeOrmFactory() {
		ormFactory.close();
	}

	@Test
	public void testORM(TestContext context) {
		final GuineaPig pig1 = new GuineaPig( 5, "Aloi" );

		try (Session session = ormFactory.openSession()) {
			session.beginTransaction();
			session.persist( pig1 );
			session.getTransaction().commit();
		}

		try (Session session = ormFactory.openSession()) {
			GuineaPig actualPig = session.find( GuineaPig.class, pig1.id );
			session.lock( actualPig, LockMode.OPTIMISTIC );
			assertThat( actualPig.version ).isEqualTo( 0 );
		}
	}

	@Entity(name = "GuineaPig")
	@Table(name = "pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;
		@Version
		private int version;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
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

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}

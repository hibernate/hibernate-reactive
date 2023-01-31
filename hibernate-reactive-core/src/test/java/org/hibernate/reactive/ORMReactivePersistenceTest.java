/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

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
		return List.of( Flour.class );
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
	public void testORMWithStageSession(TestContext context) {
		final Flour almond = new Flour( 1, "Almond", "made from ground almonds.", "Gluten free" );

		Session session = ormFactory.openSession();
		session.beginTransaction();
		session.persist( almond );
		session.getTransaction().commit();
		session.close();

		// Check database with Stage session and verify 'almond' flour exists
		test( context, openSession()
				.thenCompose( stageSession -> stageSession.find( Flour.class, almond.id ) )
				.thenAccept( entityFound -> context.assertEquals( almond, entityFound ) )
		);
	}

	@Test
	public void testORMWitMutinySession(TestContext context) {
		final Flour rose = new Flour( 2, "Rose", "made from ground rose pedals.", "Full fragrance" );

		Session ormSession = ormFactory.openSession();
		ormSession.beginTransaction();
		ormSession.persist( rose );
		ormSession.getTransaction().commit();
		ormSession.close();

		// Check database with Mutiny session and verify 'rose' flour exists
		test( context, openMutinySession()
				.chain( session -> session.find( Flour.class, rose.id ) )
				.invoke( foundRose -> context.assertEquals( rose, foundRose ) )
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

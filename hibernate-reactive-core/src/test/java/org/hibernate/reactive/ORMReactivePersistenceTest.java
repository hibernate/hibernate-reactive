/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.orm.JpaEntityManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class ORMReactivePersistenceTest extends BaseReactiveTest {
	private static final Logger logger = LoggerFactory.getLogger( ORMReactivePersistenceTest.class);

	ORMReactivePersistenceTest.Flour spelt = new ORMReactivePersistenceTest.Flour( 1, "Spelt", "An ancient grain, is a hexaploid species of wheat.", "Wheat flour" );
	ORMReactivePersistenceTest.Flour rye = new ORMReactivePersistenceTest.Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	ORMReactivePersistenceTest.Flour almond = new ORMReactivePersistenceTest.Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	private Class[] flourClasses = {ORMReactivePersistenceTest.Flour.class};

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( ORMReactivePersistenceTest.Flour.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		test( context, completedFuture( openSession() )
				.thenCompose( s -> s.persist( spelt )
						.thenCompose( v -> s.persist( rye ) )
						.thenCompose( v -> s.persist( almond ) )
						.thenCompose( v -> s.flush() ) )
		);
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, completedFuture( openSession() )
				.thenCompose( s -> s.createQuery("delete Flour").executeUpdate() ) );
	}

	@Test
	public void testSingleResultQueryOnType(TestContext context) {
		test( context, openSession().createQuery( "FROM Flour WHERE type = 'Gluten free'" )
				.getSingleResult()
				.thenAccept( flour -> context.assertEquals( almond, flour ) ));
	}

	@Test
	public void testPersistence(TestContext context)  {
		JpaEntityManagerFactory emf = null;
		try {
			emf = new JpaEntityManagerFactory( flourClasses );
			// persist the test entities and check if manager contains one of them
			EntityManager em = emf.getEntityManager();
			if( em != null ) {
				em.getTransaction().begin();
				em.persist( spelt );
				em.persist( rye );
				em.persist( almond );
				em.getTransaction().commit();
				context.assertTrue( em.contains( almond ) );
			}
		}
		catch (Exception e) {}
		finally {
			if( emf != null ) {
				emf.close();
			}
		}
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
			ORMReactivePersistenceTest.Flour flour = (ORMReactivePersistenceTest.Flour) o;
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

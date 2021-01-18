/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.ElementCollection;
//import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

public class EagerParentChildTest extends BaseReactiveTest {

	private Parent theParent;

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Parent.class );
		configuration.addAnnotatedClass( Child.class );
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty( Settings.SHOW_SQL, "true") );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty(Settings.FORMAT_SQL, "true") );
		return configuration;
	}

	@Before
	public void populateDb( TestContext context) {

		theParent = new Parent( 1111111, "Mary");
		Set<Child> children = new HashSet<>();
		children.add(new Child( "Sarah")); //, theParent));
		children.add(new Child( "Jorge")); //, theParent));
		theParent.setChildren( children );

		Stage.Session session = openSession();

		test( context,
			  session.persist( theParent )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Parent.class, theParent.getId()) )
		);
	}

	/*
	 * This test performs a simple check of Person entity collection values after persisting
	 */
	@Test
	public void findParentStageSession(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Parent.class, theParent.getId())
						.thenAccept( found -> {
							context.assertNotNull( found );
							context.assertEquals( 2, found.getChildren().size() );
						} )
		);
	}

	@Entity(name = "Parent")
	public static class Parent
	{
		/** */
		@Id
		public Integer id;

		public String name;

		@ElementCollection(fetch = FetchType.EAGER)
		public Set<Child> children;

		public Parent() { }

		/** */
		public Parent(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		/** */
		public Integer getId() { return this.id; }
		public void setId(Integer value) { this.id = value; }

		/** */
		public String getName() { return this.name; }
		public void setName(String value) { this.name = value; }

		/** */
		public Set<Child> getChildren() { return this.children; }
		public void setChildren(Set<Child> value) { this.children = value; }
	}

	@Entity(name="Child")
	public static class Child {
		/** */
		@Id
		@GeneratedValue
		public Long id;

		public String name;

		public Child() {};

		public Child(String name) {
			this.name = name;
		}

		/** */
		public String getName() { return this.name; }
		public void setName(String value) { this.name = value; }

	}

}

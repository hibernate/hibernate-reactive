/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Test that's not necessary to do a fetch when we want to add a new element to an association.
 */
public class FetchedAssociationTest extends BaseReactiveTest {

	@Rule // We use native queries, they might be different for other DBs
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	private SqlStatementTracker sqlTracker;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Child.class, Parent.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( FetchedAssociationTest::isSelectOrInsertQuery, configuration.getProperties() );
		return configuration;
	}

	private static boolean isSelectOrInsertQuery(String s) {
		return s.toLowerCase().startsWith( "select" )
				|| s.toLowerCase().startsWith( "insert" );
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	@Test
	public void testWithMutiny(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> {
					final Parent parent = new Parent();
					parent.setName( "Parent" );
					return s.persist( parent );
				} )
				.call( () -> getMutinySessionFactory()
						.withTransaction( s -> s
								.createQuery( "From Parent", Parent.class )
								.getSingleResult()
								.call( parent -> {
									Child child = new Child();
									child.setParent( parent );
									// Because we are only adding a new element, we don't need to fetch the collection
									parent.getChildren().add( child );
									return s.persist( child );
								} )
						)
				)
				.invoke( () -> assertThat( sqlTracker.getLoggedQueries() )
						.containsExactly( getExpectedNativeQueries() ) )
		);
	}

	// We don't expect a select from CHILD
	private String[] getExpectedNativeQueries() {
		return new String[] {
				"select nextval ('hibernate_sequence')",
				"insert into PARENT (name, id) values ($1, $2)",
				"select fetchedass0_.id as id1_1_, fetchedass0_.name as name2_1_ from PARENT fetchedass0_",
				"select nextval ('hibernate_sequence')",
				"insert into CHILD (name, lazy_parent_id, id) values ($1, $2, $3)"
		};
	}

	@Entity(name = "Parent")
	@Table(name = "PARENT")
	public static class Parent {
		@Id
		@GeneratedValue
		private Long id;

		private String name;

		@OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.LAZY, mappedBy = "parent")
		private List<Child> children = new ArrayList<>();

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Child> getChildren() {
			return children;
		}
	}

	@Entity(name = "Child")
	@Table(name = "CHILD")
	public static class Child {
		@Id
		@GeneratedValue
		private Long id;

		private String name;

		@ManyToOne
		@JoinColumn(name = "lazy_parent_id")
		private Parent parent;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public Parent getParent() {
			return parent;
		}

		public void setParent(Parent parent) {
			this.parent = parent;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}

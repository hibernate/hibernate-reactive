/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.LockMode;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.testing.SqlStatementTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
public class FindByIdWithLockTest extends BaseReactiveTest {
	private static final Long CHILD_ID = 1L;

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( FindByIdWithLockTest::filter, configuration.getProperties() );
		return configuration;
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean filter(String s) {
		String[] accepted = { "select " };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}


	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Parent.class, Child.class );
	}

	@Test
	public void testFindChild(VertxTestContext context) {
		Parent parent = new Parent( 1L, "Lio" );
		Child child = new Child( CHILD_ID, "And" );
		test(
				context, getMutinySessionFactory()
						.withTransaction( session -> session.persistAll( parent, child ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session
										.find( Child.class, CHILD_ID, LockModeType.PESSIMISTIC_WRITE )
										.invoke( c -> {
													 assertThat( c ).isNotNull();
													 assertThat( c.getId() ).isEqualTo( CHILD_ID );
												 }
										) ) )
		);
	}

	@Test
	public void testFindUpgradeNoWait(VertxTestContext context) {
		Child child = new Child( CHILD_ID, "And" );
		test(
				context, getMutinySessionFactory()
						.withTransaction( session -> session.persistAll( child ) )
						.invoke( () -> sqlTracker.clear() )
						.chain( () -> getMutinySessionFactory().withTransaction( session -> session
										.find( Child.class, CHILD_ID, LockMode.UPGRADE_NOWAIT )
								.invoke( c -> {
											 assertThat( c ).isNotNull();
											 assertThat( c.getId() ).isEqualTo( CHILD_ID );
											 String selectQuery = sqlTracker.getLoggedQueries().get( 0 );
											 assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
											 switch ( DatabaseConfiguration.dbType() ) {
												 case POSTGRESQL -> assertThat( selectQuery )
														 .endsWith( "for no key update of c1_0 nowait" );
												 case COCKROACHDB -> assertThat( selectQuery )
														 .endsWith( "for update of c1_0 nowait" );
												 case SQLSERVER -> assertThat( selectQuery )
														 .contains( "with (updlock,holdlock,rowlock,nowait)" );
												 case ORACLE -> assertThat( selectQuery )
														 .contains( "for update of c1_0.id nowait" );
												 case DB2 -> assertThat( selectQuery )
														 .contains( "for read only with rs use and keep update locks" ); // DB2 does not support nowait
												 case MARIA -> assertThat( selectQuery )
														 .contains( "for update nowait" );
												 case MYSQL -> assertThat( selectQuery )
														 .contains( "for update of c1_0 nowait" );
												 default -> throw new IllegalArgumentException( "Database not recognized: " + dbType().name() );
											 }
										 }
										) ) )
		);
	}


	@Entity(name = "Parent")
	public static class Parent {

		@Id
		private Long id;

		private String name;

		@OneToMany(fetch = FetchType.EAGER)
		public List<Child> children;

		public Parent() {
		}

		public Parent(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public void add(Child child) {
			if ( children == null ) {
				children = new ArrayList<>();
			}
			children.add( child );
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public List<Child> getChildren() {
			return children;
		}
	}


	@Entity(name = "Child")
	public static class Child {

		@Id
		private Long id;

		public String name;

		@ManyToOne
		public Parent parent;

		public Child() {
		}

		public Child(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Child(Long id, String name, Parent parent) {
			this.id = id;
			this.name = name;
			this.parent = parent;
			parent.add( this );
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Parent getParent() {
			return parent;
		}
	}


}

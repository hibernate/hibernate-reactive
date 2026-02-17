/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class LockTimeoutTest extends BaseReactiveTest {
	private static final Long CHILD_ID = 1L;

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.JAKARTA_LOCK_TIMEOUT, 1000 );
		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( LockTimeoutTest::selectQueryFilter, configuration.getProperties() );
		return configuration;
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean selectQueryFilter(String s) {
		return s.toLowerCase().startsWith( "select " )
				|| s.toLowerCase( Locale.ROOT ).startsWith( "set" )
				|| s.toLowerCase( Locale.ROOT ).startsWith( "show" );
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Parent.class, Child.class );
	}


	@Test
	public void testLockTimeOut(VertxTestContext context) {
		Parent parent = new Parent( 1L, "Lio" );
		Child child = new Child( CHILD_ID, "And" );
		test(
				context, getMutinySessionFactory()
						.withTransaction( session -> session.persistAll( parent, child ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction(
										session ->
												session.createQuery( "from Child c", Child.class )
														.setLockMode( LockModeType.PESSIMISTIC_WRITE )
														.getSingleResult().invoke( c -> {
															assertThat( c ).isNotNull();
															assertThat( c.getId() ).isEqualTo( CHILD_ID );
															assertTimeoutApplied();
														} )
								)
						)
		);
	}

	/**
	 * @return true if the query contains the expected the expected timeout statements
	 */
	private void assertTimeoutApplied() {
		List<String> loggedQueries = sqlTracker.getLoggedQueries();
		switch ( dbType() ) {
			case POSTGRESQL -> {
				assertThat( loggedQueries ).hasSize( 4 );
				assertThat( loggedQueries.get( 0 ).toLowerCase( Locale.ROOT ) ).isEqualTo(
						"select current_setting('lock_timeout', true)" );
				assertThat( loggedQueries.get( 1 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "set lock_timeout = 1000" );
				assertThat( loggedQueries.get( 3 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "set lock_timeout = 0" );
			}
			case COCKROACHDB -> {
				assertThat( loggedQueries ).hasSize( 4 );
				assertThat( loggedQueries.get( 0 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "show lock_timeout" );
				assertThat( loggedQueries.get( 1 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "set lock_timeout = 1000" );
				assertThat( loggedQueries.get( 3 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "set lock_timeout = 0" );
			}
			case SQLSERVER -> {
				assertThat( loggedQueries ).hasSize( 4 );
				assertThat( loggedQueries.get( 0 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "select @@lock_timeout" );
				assertThat( loggedQueries.get( 1 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "set lock_timeout 1000" );
				assertThat( loggedQueries.get( 3 ).toLowerCase( Locale.ROOT ) ).isEqualTo( "set lock_timeout -1" );
			}
			// it seems ORM has not yet enabled connection lock timeout support for MariaDB/MySQL, see MySQLLockingSupport#getLockTimeout(TimeOut)
			case MARIA, MYSQL -> assertThat( loggedQueries ).hasSize( 1 );
//			{
//					assertThat( loggedQueries ).hasSize( 4 );
//					assertThat( loggedQueries.get( 0 ).toLowerCase( Locale.ROOT ) ).isEqualTo(
//					"SELECT @@SESSION.innodb_lock_wait_timeout" );
//					assertThat( loggedQueries.get( 1 ).toLowerCase( Locale.ROOT ) ).isEqualTo(
//					"SET @@SESSION.innodb_lock_wait_timeout = 1000" );
//					assertThat( loggedQueries.get( 3 ).toLowerCase( Locale.ROOT ) ).isEqualTo(
//					"SET @@SESSION.innodb_lock_wait_timeout = 0" );
			// Oracle does not support connection lock timeout but only per-query timeouts
//		}
			case ORACLE -> {
				assertThat( loggedQueries ).hasSize( 1 );
				assertThat( loggedQueries.get( 0 ).toLowerCase( Locale.ROOT ) ).contains( "for update of c1_0.id wait 1" );
			}
			// DB2 does not support wait timeouts on locks
			case DB2 -> assertThat( loggedQueries ).hasSize( 1 );
			default -> throw new IllegalArgumentException( "Database not recognized: " + dbType().name() );
		}
	}

	@Entity(name = "Parent")
	public static class Parent {

		@Id
		private Long id;

		private String name;

		@OneToMany(fetch = FetchType.EAGER)
		public List<Child> children = new ArrayList<>();

		public Parent() {
		}

		public Parent(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public void add(Child child) {
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

		@ManyToOne(fetch = FetchType.LAZY)
		public Parent parent;

		public Child() {
		}

		public Child(Long id, String name) {
			this.id = id;
			this.name = name;
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

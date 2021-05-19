/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.hibernate:hibernate-core:5.4.31.Final
//DEPS junit:junit:4.12
//DEPS javax.persistence:javax.persistence-api:2.2
//DEPS org.hibernate.reactive:hibernate-reactive-core:1.0.0.CR4
//DEPS org.assertj:assertj-core:3.13.2
//DEPS io.vertx:vertx-pg-client:4.0.3
//DEPS io.vertx:vertx-db2-client:4.0.3
//DEPS io.vertx:vertx-mysql-client:4.0.3
//DEPS io.vertx:vertx-sql-client:4.0.3
//DEPS io.vertx:vertx-unit:4.0.3
//
//

import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test class provides a working example of setting up a simple hibernate-reactive test compatible with
 * hibernate-reactive junit tests.
 * See <a href="github.com/hibernate/hibernate-reactive/tree/main/hibernate-reactive-core/src/test/java/org/hibernate/reactive">hibernate reactive junit tests</a>
 *
 * Developers can copy/paste/edit this test to capture test failures or use-cases and attach an executable class to
 * issues or other correspondence.
 *
 * This test can be executed using the jbang CLI with the command `jbang SampleIssueTest.java`
 * See <a href="https://github.com/jbangdev/jbang">jbang</a>"
 * jbang will compile and execute a jar generated based on dependencies defined above in the
 * file header (i.e.  //DEPS org.hibernate:hibernate-core:5.4.31.Final )
 *
 *  >  Customize your JDBC connection properties
 *  >  Define your specific entity classes and relationships
 *  >  Replicate your issue and or exception
 *  >  attach the resulting file or a URL link to it
 *
 *  Note this test utilizes a VertxUnitRunner class which provides the hooks to the reactive
 *  framework for transaction control.
 *
 */
@RunWith(VertxUnitRunner.class)
public class SampleIssueTest {

	private Mutiny.SessionFactory sessionFactory;

	/**
	 * Define the configuration parameter values for your use-case
	 */
	private Configuration createConfiguration() {
		Configuration configuration = new Configuration();

		// Use the correct JDBC url
		configuration.setProperty( Settings.URL, "jdbc:postgresql://localhost:5432/hreact" );

		// =====  OTHER Test DBs =======================================================================
		// MYSQL: jdbc:mysql://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC
		// DB2:   jdbc:db2://localhost:50000/hreact:user=hreact;password=hreact;
		// CockroachDB  jdbc:cockroachdb://localhost:26257/postgres?sslmode=disable&user=root
		//  NOTE:  CockroachDB does not need password and requires //DEPS io.vertx:vertx-pg-client:4.0.3
		// ==============================================================================================

		// Credentials
		configuration.setProperty( Settings.USER, "hreact");
		configuration.setProperty( Settings.PASS, "hreact");

		// Schema generation
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );

		// Add additional entities here
		configuration.addAnnotatedClass(MyEntity.class);

		// Query logging
		configuration.setProperty( Settings.SHOW_SQL, "true" );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, "true" );
		configuration.setProperty( Settings.FORMAT_SQL, "true" );
		return  configuration;
	}

	@Before
	public void createSessionFactory() {
		Configuration configuration = createConfiguration();
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() );
		StandardServiceRegistry registry = builder.build();

		sessionFactory = configuration.buildSessionFactory( registry )
				.unwrap( Mutiny.SessionFactory.class );
	}

	@Test
	public void firstTest(TestContext context) {
		Async async = context.async();

		MyEntity entity = new MyEntity( "first entity", 1 );
		sessionFactory
				.withTransaction( (session, tx) -> session.persist( entity ) )
				.chain( () -> sessionFactory
						.withSession( session -> session
								.find( MyEntity.class, entity.getId() )
								.invoke( foundEntity -> assertThat( foundEntity.getName() ).isEqualTo( entity.getName() ) ) ) )
				.subscribe().with( res -> async.complete(), throwable -> context.fail( throwable )
		);
	}

	@After
	public void closeFactory() {
		if ( sessionFactory != null ) {
			sessionFactory.close();
		}
	}

	/*
	 * Define your hibernate entity classes and relationships.
	 *
	 * This initial test includes a single entity but you can create joined and/or embedded entities too
	 *
	 * Be sure to add all annotated entity classes in the createConfiguration() method above
	 *    example: configuration.addAnnotatedClass(MyOtherEntity.class);
	 */
	@Entity(name = "MyEntity")
	public static class MyEntity  {
		@Id
		public Integer id;

		public String name;

		public MyEntity() {}

		public MyEntity(String name, Integer id) {
			this.name = name;
			this.id = id;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "MyEntity"
					+ "\n\t id = " + id
					+ "\n\t name = " + name;
		}
	}

	public static void main(String[] args) {
		Result result = JUnitCore.runClasses( SampleIssueTest.class);

		if( result.wasSuccessful() ) {
			System.out.println( "All unit tests passed");
		} else {
			for ( Failure failure : result.getFailures() ) {
				System.out.println( "TEST FAILURE: " + failure.toString() );
			}
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS io.vertx:vertx-pg-client:${vertx.version:4.2.4}
//DEPS com.ongres.scram:client:2.1
//DEPS io.vertx:vertx-db2-client:${vertx.version:4.2.4}
//DEPS io.vertx:vertx-mysql-client:${vertx.version:4.2.4}
//DEPS io.vertx:vertx-unit:${vertx.version:4.2.4}
//DEPS org.hibernate.reactive:hibernate-reactive-core:${hibernate-reactive.version:1.0.0.CR6}
//DEPS org.assertj:assertj-core:3.19.0
//DEPS junit:junit:4.13.2
//DEPS org.testcontainers:postgresql:1.15.3
//DEPS org.testcontainers:mysql:1.15.3
//DEPS org.testcontainers:db2:1.15.3
//DEPS org.testcontainers:mariadb:1.15.3
//DEPS org.testcontainers:cockroachdb:1.15.3
//
//// Testcontainer needs the JDBC drivers to start the containers
//// Hibernate Reactive doesn't use them
//DEPS org.postgresql:postgresql:42.2.16
//DEPS mysql:mysql-connector-java:8.0.28
//DEPS org.mariadb.jdbc:mariadb-java-client:2.7.3
//

import java.util.function.Supplier;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An example of a JUnit test class for Hibernate Reactive using
 * <a hreaf="https://vertx.io/docs/vertx-unit/java/">Vert.x Unit</a> and
 * <a hreaf="https://www.testcontainers.org/">Testcontainers</a>
 * that you can run using <a hreaf="https://www.jbang.dev/">JBang</a>.
 * <p>
 * Before running the tests, Testcontainers will start the selected
 * Docker image with the required database created.
 * </p>
 * <p>
 * The {@link #DATABASE} constant define which database to use and
 * it can be change to any of the values in {@link Database}.
 * </p>
 * <p>
 * Usage example:
 *     <dl>
 *         <dt>1. Download JBang</dt>
 *         <dd>See <a hreaf="https://www.jbang.dev/download">https://www.jbang.dev/download</a></dd>
 *         <dt>2. Run the test with JBang</dt>
 *         <dd>
 *             <pre>jbang ReactiveTest.java</pre>
 *         </dd>
 *         <dt>3. (Optional) Edit the file (with IntelliJ IDEA for example):</dt>
 *         <dd>
 *             <pre>jbang edit --open=idea SampleIssueTest.java</pre>
 *         </dd>
 *     </dl>
 * <p/>
 * <p>
 *     Note that in a real case scenario, you would only need the dependencies
 *     for the database of your choice.
 * </p>
 */
@RunWith(VertxUnitRunner.class)
public class ReactiveTest {

	/**
	 * The database to use for the tests
	 */
	public final static Database DATABASE = Database.POSTGRESQL;

	private static JdbcDatabaseContainer<?> container;

	private Mutiny.SessionFactory sessionFactory;

	@BeforeClass
	public static void startContainer() {
		container = DATABASE.startContainer();
	}

	/**
	 * The {@link Configuration} for the {@link Mutiny.SessionFactory}.
	 */
	private Configuration createConfiguration() {
		Configuration configuration = new Configuration();

		// JDBC url
		configuration.setProperty( Settings.URL, container.getJdbcUrl() );

		// Credentials
		configuration.setProperty( Settings.USER, container.getUsername() );
		configuration.setProperty( Settings.PASS, container.getPassword() );

		// Schema generation. Supported values are create, drop, create-drop, drop-create, none
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );

		// Register new entity classes here
		configuration.addAnnotatedClass( MyEntity.class );

		// (Optional) Log the SQL queries
		configuration.setProperty( Settings.SHOW_SQL, "true" );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, "true" );
		configuration.setProperty( Settings.FORMAT_SQL, "true" );
		return configuration;
	}

	/*
	 * Create a new factory and a new schema before each test (see
	 * property `hibernate.hbm2ddl.auto`).
	 * This way each test will start with a clean database.
	 *
	 * The drawback is that, in a real case scenario with multiple tests,
	 * it can slow down the whole test suite considerably. If that happens,
	 * it's possible to make the session factory static and, if necessary,
	 * delete the content of the tables manually (without dropping them).
	 */
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
	public void testInsertAndSelect(TestContext context) {
		// the test will wait until async.complete or context.fail are called
		Async async = context.async();

		MyEntity entity = new MyEntity( "first entity", 1 );
		sessionFactory
				// insert the entity in the database
				.withTransaction( (session, tx) -> session.persist( entity ) )
				.chain( () -> sessionFactory
						.withSession( session -> session
								// look for the entity by id
								.find( MyEntity.class, entity.getId() )
								// assert that the returned entity is the right one
								.invoke( foundEntity -> assertThat( foundEntity.getName() ).isEqualTo( entity.getName() ) ) ) )
				.subscribe()
				.with( res -> async.complete(), context::fail );
	}

	@After
	public void closeFactory() {
		if ( sessionFactory != null ) {
			sessionFactory.close();
		}
	}

	/**
	 * Example of a class representing an entity.
	 * <p>
	 * If you create new entities, be sure to add them in {@link #createConfiguration()}.
	 * For example:
	 * <pre>
	 * configuration.addAnnotatedClass( MyOtherEntity.class );
	 * </pre>
	 */
	@Entity(name = "MyEntity")
	public static class MyEntity {
		@Id
		public Integer id;

		public String name;

		public MyEntity() {
		}

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

	/**
	 * The only purpose of this class is to make it easier to switch among the available databases
	 * for this unit test.
	 * <p>
	 * It's a wrapper around the testcontainers classes.
	 */
	enum Database {
		POSTGRESQL( () -> new PostgreSQLContainer( "postgres:14" ) ),
		MYSQL( () -> new MySQLContainer( "mysql:8.0.28" ) ),
		DB2( () -> new Db2Container( "ibmcom/db2:11.5.5.1" ).acceptLicense() ),
		MARIADB( () -> new MariaDBContainer( "mariadb:10.7.1" ) ),
		COCKROACHDB( () -> new CockroachContainer( "cockroachdb/cockroach:v21.2.4" ) );

		private final Supplier<JdbcDatabaseContainer<?>> containerSupplier;

		Database(Supplier<JdbcDatabaseContainer<?>> supplier) {
			containerSupplier = supplier;
		}

		public JdbcDatabaseContainer<?> startContainer() {
			JdbcDatabaseContainer<?> jdbcDatabaseContainer = containerSupplier.get();
			jdbcDatabaseContainer
					.withReuse( true )
					.start();
			return jdbcDatabaseContainer;
		}
	}

	// This main class is only for JBang so that it can run the tests with `jbang ReactiveTest`
	public static void main(String[] args) {
		System.out.println( "Starting the test suite with " + DATABASE );

		Result result = JUnitCore.runClasses( ReactiveTest.class );

		for ( Failure failure : result.getFailures() ) {
			System.out.println();
			System.err.println( "Test " + failure.getTestHeader() + " FAILED!" );
			System.err.println( "\t" + failure.getTrace() );
		}

		System.out.println();
		System.out.print("Tests result summary: ");
		System.out.println( result.wasSuccessful() ? "SUCCESS" : "FAILURE" );
	}
}

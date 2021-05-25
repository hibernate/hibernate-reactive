/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS junit:junit:4.12
//DEPS javax.persistence:javax.persistence-api:2.2
//DEPS org.hibernate.reactive:hibernate-reactive-core:1.0.0.CR5
//DEPS org.assertj:assertj-core:3.13.2
//DEPS io.vertx:vertx-pg-client:4.1.0.CR1
//DEPS io.vertx:vertx-mysql-client:4.1.0.CR1
//DEPS io.vertx:vertx-db2-client:4.1.0.CR1
//DEPS io.vertx:vertx-sql-client:4.1.0.CR1
//DEPS io.vertx:vertx-unit:4.1.0.CR1
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
 * JBang compatible JUnit test class for Hibernate Reactive.
 * <p>
 * The test expect the selected database (the default is PostgreSQL)
 * to be up and running. Instructions on how to start the databases
 * using Podman or Docker are in the
 * <a hreaf="https://github.com/hibernate/hibernate-reactive/blob/main/podman.md">podman.md</a>
 * file in the Hibernate Reactive project on GitHub.
 * </p>
 * <p>
 * Usage example:
 *     <dl>
 *         <dt>1. Download JBang</dt>
 *         <dd>See <a hreaf="https://www.jbang.dev/download">https://www.jbang.dev/download</a></dd>
 *         <dt>2. Start the database with the right credentials</dt>
 *         <dd>
 *             Using Podman or Docker (you can replace {@code podman} with {@code docker}):
 *             <pre>
 * podman run --rm --name HibernateTestingPGSQL \
 *     -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact \
 *     -p 5432:5432 postgres:13.2
 *             </pre>
 *         </dd>
 *         <dt>3. Run the test with JBang</dt>
 *         <dd>
 *             <pre>jbang SampleIssueTest.java</pre>
 *         </dd>
 *         <dt>4. (Optional) Edit the file (with IntelliJ IDEA for example):</dt>
 *         <dd>
 *             <pre>jbang edit --open=idea SampleIssueTest.java</pre>
 *         </dd>
 *     </dl>
 * <p/>
 *
 * @see <a href="https://www.jbang.dev/">JBang</a>
 * @see <a href="https://vertx.io/docs/vertx-unit/java/#_junit_integration">Vert.x Unit</a>
 */
@RunWith(VertxUnitRunner.class)
public class SampleIssueTest {

	private Mutiny.SessionFactory sessionFactory;

	/**
	 * The default URLs for the supported databases
 	 */
	enum Database {
		POSTGRESQL( "jdbc:postgresql://localhost:5432/hreact?user=hreact&password=hreact" ),
		MYSQL( "jdbc:mysql://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC" ),
		MARIADB( "jdbc:mariadb://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC" ),
		DB2( "jdbc:db2://localhost:50000/hreact:user=hreact;password=hreact;" ),
		COCKROACHDB( "jdbc:cockroachdb://localhost:26257/postgres?sslmode=disable&user=root" );

		private String url;

		Database(String url) {
			this.url = url;
		}

		public String getUrl() {
			return url;
		}
	}

	/**
	 * Create the {@link Configuration} for the {@link Mutiny.SessionFactory}.
	 */
	private Configuration createConfiguration() {
		Configuration configuration = new Configuration();

		configuration.setProperty( Settings.URL, Database.POSTGRESQL.getUrl() );

		// (Optional) Override default credentials
		// configuration.setProperty( Settings.USER, "hreact" );
		// configuration.setProperty( Settings.PASS, "hreact" );

		// Supported values are: none, create, create-drop
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );

		// Register new entities here
		configuration.addAnnotatedClass( MyEntity.class );

		// Query logging
		configuration.setProperty( Settings.SHOW_SQL, "true" );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, "true" );
		configuration.setProperty( Settings.FORMAT_SQL, "true" );
		return configuration;
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
	 * This is so that JBang can run the tests.
	 */
	public static void main(String[] args) {
		Result result = JUnitCore.runClasses( SampleIssueTest.class );

		if ( result.wasSuccessful() ) {
			System.out.println( "All unit tests passed" );
		}
		else {
			for ( Failure failure : result.getFailures() ) {
				System.out.println( "FAILURE: " + failure.toString() );
			}
		}
	}
}

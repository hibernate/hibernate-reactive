/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.persistence.criteria.CriteriaQuery;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Similar to BaseReactiveTest in the hibernate-reactive-core.
 * Hopefully, one day we will reorganize the code better.
 */
@RunWith(VertxUnitRunner.class)
public abstract class BaseReactiveIT {

	// These properties are in DatabaseConfiguration in core
	public static final boolean USE_DOCKER = Boolean.getBoolean( "docker" );

	public static final String IMAGE_NAME = "postgres:15.0";
	public static final String USERNAME = "hreact";
	public static final String PASSWORD = "hreact";
	public static final String DB_NAME = "hreact";

	public static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>( IMAGE_NAME )
			.withUsername( USERNAME )
			.withPassword( PASSWORD )
			.withDatabaseName( DB_NAME )
			.withReuse( true );

	@ClassRule
	public static Timeout rule = Timeout.seconds( 10 * 60 );

	private static SessionFactory ormSessionFactory;
	@ClassRule
	public static RunTestOnContext vertxContextRule = new RunTestOnContext( () -> {
		VertxOptions options = new VertxOptions();
		options.setBlockedThreadCheckInterval( 5 );
		options.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return Vertx.vertx( options );
	} );

	/**
	 * Configure properties defined in {@link Settings}.
	 */
	protected void setProperties(Configuration configuration) {
		setDefaultProperties( configuration );
	}

	/**
	 * Configure default properties common to most tests.
	 */
	public static void setDefaultProperties(Configuration configuration) {
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.URL, dbConnectionUrl( USE_DOCKER ) );
		configuration.setProperty( Settings.USER, USERNAME );
		configuration.setProperty( Settings.PASS, PASSWORD );

		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty( Settings.SHOW_SQL, "true" ) );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty( Settings.FORMAT_SQL, "false" ) );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, System.getProperty( Settings.HIGHLIGHT_SQL, "true" ) );
	}

	private static String dbConnectionUrl(boolean enableDocker) {
		if ( enableDocker ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			postgresql.start();
			return postgresql.getJdbcUrl();
		}

		// When we don't use testcontainers we expect a database on the default url
		return "postgres://localhost:5432/" + DB_NAME;
	}

	protected static void test(TestContext context, CompletionStage<?> work) {
		test( context.async(), context, work );
	}

	/**
	 * These entities will be added to the configuration of the factory and
	 * the rows in the mapping tables deleted after each test.
	 * <p>
	 * For more complicated configuration see {@link #constructConfiguration()}
	 * or {@link #cleanDb()}.
	 */
	protected Collection<Class<?>> annotatedEntities() {
		return List.of();
	}

	/**
	 * XML mapping documents to be added to the configuration.
	 */
	protected Collection<String> mappings() {
		return List.of();
	}

	/**
	 * For when we need to create the {@link Async} in advance
	 */
	protected static void test(Async async, TestContext context, CompletionStage<?> work) {
		work.whenComplete( (res, err) -> {
			if ( err != null ) {
				context.fail( err );
			}
			else {
				async.complete();
			}
		} );
	}

	protected static void test(TestContext context, Uni<?> uni) {
		test( context.async(), context, uni );
	}

	/**
	 * For when we need to create the {@link Async} in advance
	 */
	public static void test(Async async, TestContext context, Uni<?> uni) {
		uni.subscribe().with( res -> async.complete(), context::fail );
	}

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		addEntities( configuration );
		setProperties( configuration );
		return configuration;
	}

	protected void addEntities(Configuration configuration) {
		for ( Class<?> entity: annotatedEntities() ) {
			configuration.addAnnotatedClass( entity );
		}
		for ( String mapping: mappings() ) {
			configuration.addResource( mapping );
		}
	}

	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return getSessionFactory()
				.withTransaction( s -> loop( entities, entityClass -> s
						.createQuery( queryForDelete( entityClass ) )
						.getResultList()
						// This approach will also remove embedded collections and associated entities when possible
						// (a `delete from` query will only remove the elements from one table)
						.thenCompose( list -> s.remove( list.toArray( new Object[0] ) ) )
				) );
	}

	private <T> CriteriaQuery<T> queryForDelete(Class<T> entityClass) {
		final CriteriaQuery<T> query = getSessionFactory().getCriteriaBuilder().createQuery( entityClass );
		query.from( entityClass );
		return query;
	}

	@Before
	public void before(TestContext context) {
		test( context, setupSessionFactory( this::constructConfiguration ) );
	}


	/**
	 * Set up one session factory shared by the tests in the class.
	 */
	protected CompletionStage<Void> setupSessionFactory(Configuration configuration) {
		return setupSessionFactory( () -> configuration );
	}

	/**
	 * Set up the session factory but create the configuration only if necessary.
	 *
	 * @param confSupplier supplies the configuration for the factory
	 * @return a {@link CompletionStage} void that succeeds when the factory is ready.
	 */
	protected CompletionStage<Void> setupSessionFactory(Supplier<Configuration> confSupplier) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		vertxContextRule.vertx()
				.executeBlocking(
						// schema generation is a blocking operation and so it causes an
						// exception when run on the Vert.x event loop. So call it using
						// Vertx.executeBlocking()
						promise -> startFactoryManager( promise, confSupplier ),
						event -> {
							if ( event.succeeded() ) {
								future.complete( null );
							}
							else {
								future.completeExceptionally( event.cause() );
							}
						}
				);
		return future;
	}

	private void startFactoryManager(Promise<Object> p, Supplier<Configuration> confSupplier ) {
		try {
			ormSessionFactory = createHibernateSessionFactory( confSupplier.get() );
			p.complete();
		}
		catch (Throwable e) {
			p.fail( e );
		}
	}

	private SessionFactory createHibernateSessionFactory(Configuration configuration) {
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() );
		addServices( builder );
		StandardServiceRegistry registry = builder.build();
		configureServices( registry );
		return configuration.buildSessionFactory( registry );
	}

	protected void addServices(StandardServiceRegistryBuilder builder) {}

	protected void configureServices(StandardServiceRegistry registry) {
	}

	@After
	public void after(TestContext context) {
		test( context, cleanDb() );
	}

	/**
	 * Called after each test, remove all the entities defined in {@link #annotatedEntities()}
	 */
	protected CompletionStage<Void> cleanDb() {
		final Collection<Class<?>> classes = annotatedEntities();
		return classes.isEmpty()
				? voidFuture()
				: deleteEntities( classes.toArray( new Class<?>[0] ) );
	}

	@AfterClass
	public static void closeFactory(TestContext context) {
		if ( ormSessionFactory != null && ormSessionFactory.isOpen() ) {
			ormSessionFactory.close();
		}
	}

	protected static Stage.SessionFactory getSessionFactory() {
		return ormSessionFactory.unwrap( Stage.SessionFactory.class );
	}


	protected static Mutiny.SessionFactory getMutinySessionFactory() {
		return ormSessionFactory.unwrap( Mutiny.SessionFactory.class );
	}
}

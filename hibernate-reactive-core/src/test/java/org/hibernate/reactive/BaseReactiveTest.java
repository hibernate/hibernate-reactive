/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.provider.service.ReactiveGenerationTarget;
import org.hibernate.reactive.provider.service.ReactiveSchemaManagementTool;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

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

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Base class for unit tests that need a connection to the selected db and
 * need to wait for the end of the task for the assertion.
 * <p>
 *     Uses the {@link RunTestOnContext} rule to guarantee that all tests
 *     will run in the Vert.x event loop thread (by default, the tests using Vert.x unit will start
 *     in the JUnit thread).
 * </p>
 * <p>
 *     Contains several utility methods to make it easier to test Hibernate Reactive
 *     using Vert.x unit.
 * </p>
 */
@RunWith(VertxUnitRunner.class)
public abstract class BaseReactiveTest {

	public static SessionFactoryManager factoryManager = new SessionFactoryManager();

	@ClassRule
	public static Timeout rule = Timeout.seconds( 10 * 60 );

	@ClassRule
	public static RunTestOnContext vertxContextRule = new RunTestOnContext( () -> {
		VertxOptions options = new VertxOptions();
		options.setBlockedThreadCheckInterval( 5 );
		options.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return Vertx.vertx( options );
	} );

	private Object session;
	private Object statelessSession;

	private ReactiveConnection connection;

	protected static void test(TestContext context, CompletionStage<?> work) {
		test( context.async(), context, work );
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
		uni.subscribe().with(
				res -> async.complete(),
				context::fail
		);
	}

	private static boolean doneTablespace;

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getJdbcUrl() );
		if ( DatabaseConfiguration.dbType() == DBType.DB2 && !doneTablespace ) {
			configuration.setProperty(Settings.HBM2DDL_IMPORT_FILES, "/db2.sql");
			doneTablespace = true;
		}
		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "false") );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty(Settings.FORMAT_SQL, "false") );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, System.getProperty(Settings.HIGHLIGHT_SQL, "true") );
		return configuration;
	}

	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return deleteEntities( Arrays.stream( entities )
									   .map( BaseReactiveTest::defaultEntityName )
									   .collect( Collectors.toList() )
									   .toArray( new String[entities.length] ) );
	}

	private static String defaultEntityName(Class<?> aClass) {
		int index = aClass.getName().lastIndexOf( '.' );
		index = index > -1 ? index + 1 : 0;
		return aClass.getName().substring( index );
	}

	public CompletionStage<Void> deleteEntities(String... entities) {
		return getSessionFactory()
				.withTransaction( (s, tx) -> loop( entities, name -> s
						.createQuery( "from " + name ).getResultList()
						.thenCompose( list -> s.remove( list.toArray( new Object[0] ) ) ) ) );
	}

	@Before
	public void before(TestContext context) {
		test( context, setupSessionFactory( constructConfiguration() ) );
	}

	protected CompletionStage<Void> setupSessionFactory(Configuration configuration) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		vertxContextRule.vertx()
				.executeBlocking(
						// schema generation is a blocking operation and so it causes an
						// exception when run on the Vert.x event loop. So call it using
						// Vertx.executeBlocking()
						promise -> startFactoryManager( promise, configuration ),
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

	private void startFactoryManager(Promise<Object> p, Configuration configuration ) {
		try {
			factoryManager.start( () -> createHibernateSessionFactory( configuration ) );
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

	/*
	 * MySQL doesn't implement 'drop table cascade constraints'.
	 *
	 * The reason this is a problem in our test suite is that we
	 * have lots of different schemas for the "same" table: Pig, Author, Book.
	 * A user would surely only have one schema for each table.
	 */
	protected void configureServices(StandardServiceRegistry registry) {
		if ( dbType() == DBType.MYSQL ) {
			registry.getService( ConnectionProvider.class ); //force the NoJdbcConnectionProvider to load first
			ReactiveSchemaManagementTool tool = (ReactiveSchemaManagementTool) registry.getService(
					SchemaManagementTool.class
			);
			tool.setCustomDatabaseGenerationTarget( new ReactiveGenerationTarget(registry) {
				@Override
				public void prepare() {
					super.prepare();
					accept("set foreign_key_checks = 0");
				}
				@Override
				public void release() {
					accept("set foreign_key_checks = 1");
					super.release();
				}
			} );
		}
	}

	@After
	public void after(TestContext context) {
		test( context, closeSession( session )
				.thenAccept( v -> session = null )
				.thenCompose( v -> closeSession( connection ) )
				.thenAccept( v -> connection = null ) );
	}

	protected static CompletionStage<Void> closeSession(Object closable) {
		if ( closable instanceof CompletionStage<?> ) {
			CompletionStage<?> closableStage = (CompletionStage<?>) closable;
			return closableStage.thenCompose( BaseReactiveTest::closeSession );
		}
		if ( closable instanceof Uni<?> ) {
			Uni<?> closableUni = (Uni<?>) closable;
			return closableUni.subscribeAsCompletionStage()
					.thenCompose( BaseReactiveTest::closeSession );
		}
		if ( closable instanceof ReactiveConnection ) {
			return ( (ReactiveConnection) closable ).close();
		}
		if ( closable instanceof Mutiny.Session ) {
			Mutiny.Session mutiny = (Mutiny.Session) closable;
			if ( mutiny.isOpen() ) {
				return mutiny.close().subscribeAsCompletionStage();
			}
		}
		if ( closable instanceof Stage.Session ) {
			Stage.Session stage = (Stage.Session) closable;
			if ( stage.isOpen() ) {
				return stage.close();
			}
		}
		if ( closable instanceof Mutiny.StatelessSession ) {
			Mutiny.StatelessSession mutiny = (Mutiny.StatelessSession) closable;
			if ( mutiny.isOpen() ) {
				return mutiny.close().subscribeAsCompletionStage();
			}
		}
		if ( closable instanceof Stage.StatelessSession ) {
			Stage.StatelessSession stage = (Stage.StatelessSession) closable;
			if ( stage.isOpen() ) {
				return stage.close();
			}
		}
		return voidFuture();
	}

	@AfterClass
	public static void closeFactory(TestContext context) {
		test( context, factoryManager.stop() );
	}

	protected static Stage.SessionFactory getSessionFactory() {
		return factoryManager.getHibernateSessionFactory().unwrap( Stage.SessionFactory.class );
	}

	/**
	 * Close the existing open session and create a new {@link Stage.Session}
	 *
	 * @return a new Stage.Session
	 */
	protected CompletionStage<Stage.Session> openSession() {
		return closeSession( session )
				.thenCompose( v -> getSessionFactory().openSession()
						.thenApply( this::saveSession ) );
	}

	/**
	 * Close the existing open session and create a new {@link Stage.StatelessSession}
	 *
	 * @return a new Stage.StatelessSession
	 */
	protected CompletionStage<Stage.StatelessSession> openStatelessSession() {
		return closeSession( statelessSession )
				.thenCompose( v -> getSessionFactory().openStatelessSession()
						.thenApply( this::saveStatelessSession ) );
	}

	protected CompletionStage<ReactiveConnection> connection() {
		return factoryManager.getReactiveConnectionPool().getConnection().thenApply( c -> connection = c );
	}

	/**
	 * Close the existing open session and create a new {@link Mutiny.Session}
	 *
	 * @return a new Mutiny.Session
	 */
	protected Uni<Mutiny.Session> openMutinySession() {
		return Uni.createFrom().completionStage( closeSession( session ) )
				.replaceWith( getMutinySessionFactory().openSession().invoke( this::saveSession ) );
	}

	/**
	 * Close the existing open session and create a new {@link Mutiny.StatelessSession}
	 *
	 * @return a new Mutiny.StatelessSession
	 */
	protected Uni<Mutiny.StatelessSession> openMutinyStatelessSession() {
		return Uni.createFrom().completionStage( closeSession( statelessSession ) )
				.replaceWith( getMutinySessionFactory().openStatelessSession().invoke( this::saveStatelessSession ) );
	}

	protected static Mutiny.SessionFactory getMutinySessionFactory() {
		return factoryManager.getHibernateSessionFactory().unwrap( Mutiny.SessionFactory.class );
	}

	private <T> T saveSession(T newSession) {
		this.session = newSession;
		return newSession;
	}

	private <T> T saveStatelessSession(T newSession) {
		this.statelessSession = newSession;
		return newSession;
	}
}

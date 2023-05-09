/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.provider.service.ReactiveGenerationTarget;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Promise;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.RunTestOnContext;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.criteria.CriteriaQuery;

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
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = 600, timeUnit = TimeUnit.SECONDS)
public abstract class BaseReactiveTest {
	/**
	 * Configure Vertx JUnit5 test context
	 */
	@RegisterExtension
	static RunTestOnContext testOnContext = new RunTestOnContext( vertxOptions() );

	private static VertxOptions vertxOptions() {
		return new VertxOptions()
				.setBlockedThreadCheckInterval( 5 )
				.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
	}

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
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getJdbcUrl() );
		if ( DatabaseConfiguration.dbType() == DBType.DB2 && !doneTablespace ) {
			configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES, "/db2.sql" );
			doneTablespace = true;
		}
		// Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		// Keep the default to false, otherwise the log on CI becomes too big
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty( Settings.SHOW_SQL, "false" ) );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty( Settings.FORMAT_SQL, "false" ) );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, System.getProperty( Settings.HIGHLIGHT_SQL, "true" ) );
	}

	public static final SessionFactoryManager factoryManager = new SessionFactoryManager();

	private Object session;
	private Object statelessSession;

	private ReactiveConnection connection;

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

	public static void test(VertxTestContext context, CompletionStage<?> work) {
		work.whenComplete( (res, err) -> {
			if ( err != null ) {
				context.failNow( err );
			}
			else {
				context.completeNow();
			}
		} );
	}

	public static void test(VertxTestContext context, Uni<?> uni) {
		uni.subscribe().with( res -> context.completeNow(), context::failNow );
	}

	private static boolean doneTablespace;

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		addEntities( configuration );
		setProperties( configuration );
		return configuration;
	}

	protected void addEntities(Configuration configuration) {
		for ( Class<?> entity : annotatedEntities() ) {
			configuration.addAnnotatedClass( entity );
		}
		for ( String mapping : mappings() ) {
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

	@BeforeEach
	public void before(VertxTestContext context) {
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
	 *
	 * @return a {@link CompletionStage} void that succeeds when the factory is ready.
	 */
	protected CompletionStage<Void> setupSessionFactory(Supplier<Configuration> confSupplier) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		testOnContext.vertx()
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

	private void startFactoryManager(Promise<Object> p, Supplier<Configuration> confSupplier) {
		try {
			factoryManager.start( () -> createHibernateSessionFactory( confSupplier.get() ) );
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

	protected void addServices(StandardServiceRegistryBuilder builder) {
	}

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
			SchemaManagementTool tool = registry.getService( SchemaManagementTool.class );
			tool.setCustomDatabaseGenerationTarget( new ReactiveGenerationTarget( registry ) {
				@Override
				public void prepare() {
					super.prepare();
					accept( "set foreign_key_checks = 0" );
				}

				@Override
				public void release() {
					accept( "set foreign_key_checks = 1" );
					super.release();
				}
			} );
		}
	}

	@AfterEach
	public void after(VertxTestContext context) {
		test( context, closeSession( session )
				.thenAccept( v -> session = null )
				.thenCompose( v -> closeSession( statelessSession ) )
				.thenAccept( v -> statelessSession = null )
				.thenCompose( v -> closeSession( connection ) )
				.thenAccept( v -> connection = null )
				.thenCompose( v -> cleanDb() )
		);
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

	@AfterAll
	public static void closeFactory(VertxTestContext context) {
		test( context, factoryManager.stop() );
	}

	protected static Stage.SessionFactory getSessionFactory() {
		return factoryManager.getHibernateSessionFactory().unwrap( Stage.SessionFactory.class );
	}

	protected static Dialect getDialect() {
		return factoryManager.getHibernateSessionFactory()
				.unwrap( SessionFactoryImplementor.class )
				.getJdbcServices()
				.getDialect();
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

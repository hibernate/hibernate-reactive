/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.reactive.vertx.VertxInstance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.fail;
import static org.hibernate.cfg.AvailableSettings.SHOW_SQL;
import static org.hibernate.reactive.BaseReactiveTest.setDefaultProperties;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.provider.Settings.POOL_CONNECT_TIMEOUT;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * This is a multi-threaded stress test, intentionally consuming some time
 * that also opens the connection lazily.
 * The purpose is to verify that the sequence optimizer used by Hibernate Reactive
 * is indeed able to generate unique IDs backed by the database sequences, while
 * running multiple operations in different threads and on multiple Vert.x eventloops.
 * This is very similar to MultithreadedIdentityGenerationTest except it models
 * the full operations including the insert statements, while the latter focuses
 * on the generated IDs to be unique; it's useful to maintain both tests as:
 *  - ID generation needs to be unique so it's good to stress that aspect
 *    in isolation
 *  - insert operations are downstream events, so this allows us to test that
 *    such downstream events are not being unintentionally duplicated/dropped,
 *    which could actually happen when the id generator triggers unintended
 *    threading behaviours.
 *
 * N.B. We actually had a case in which the IDs were uniquely generated but the
 * downstream event was being processed twice (or more) concurrently, so it's
 * useful to have both integration tests.
 *
 * A typical reactive application will not require multiple threads, but we
 * specifically want to test for the case in which the single ID source is being
 * shared across multiple threads and also multiple eventloops.
 * @see MultithreadedInsertionTest
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = MultithreadedInsertionWithLazyConnectionTest.TIMEOUT_MINUTES, timeUnit = MINUTES)
public class MultithreadedInsertionWithLazyConnectionTest {

	/**
	 * The number of threads should be higher than the default size of the connection pool so that
	 * this test is also effective in detecting problems with resource starvation.
	 */
	private static final int N_THREADS = 12;
	private static final int ENTITIES_STORED_PER_THREAD = 2000;

	//Should finish much sooner, but generating this amount of IDs could be slow on some CIs
	public static final int TIMEOUT_MINUTES = 10;

	// Keeping this disabled because it generates a lot of queries
	private static final boolean LOG_SQL = false;

	/**
	 * If true, it will print info about the threads
	 */
	private static final boolean THREAD_PRETTY_MSG = true;

	private static final Latch startLatch = new Latch( "start", N_THREADS );
	private static final Latch endLatch = new Latch( "end", N_THREADS );

	private static Stage.SessionFactory stageSessionFactory;
	private static Vertx vertx;
	private static SessionFactory sessionFactory;

	@BeforeEach
	public void setupSessionFactory() {
		vertx = Vertx.vertx( getVertxOptions() );
		Configuration configuration = new Configuration();
		setDefaultProperties( configuration );
		configuration.addAnnotatedClass( EntityWithGeneratedId.class );
		configuration.setProperty( SHOW_SQL, String.valueOf( LOG_SQL ) );
		configuration.setProperty( POOL_CONNECT_TIMEOUT, String.valueOf( TIMEOUT_MINUTES * 60 * 1000 ) );
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() )
				//Inject our custom vert.x instance:
				.addService( VertxInstance.class, () -> vertx );
		StandardServiceRegistry registry = builder.build();
		sessionFactory = configuration.buildSessionFactory( registry );
		stageSessionFactory = sessionFactory.unwrap( Stage.SessionFactory.class );
	}

	private static VertxOptions getVertxOptions() {
		final VertxOptions vertxOptions = new VertxOptions();
		vertxOptions.setEventLoopPoolSize( N_THREADS );
		//We relax the blocked thread checks as we'll actually use latches to block them
		//intentionally for the purpose of the test; functionally this isn't required
		//but it's useful as self-test in the design of this, to ensure that the way
		//things are setup are indeed being run in multiple, separate threads.
		vertxOptions.setBlockedThreadCheckInterval( TIMEOUT_MINUTES );
		vertxOptions.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return vertxOptions;
	}

	@AfterEach
	public void closeSessionFactory() {
		stageSessionFactory.close();
	}

	@Test
	public void testIdentityGenerator(VertxTestContext context) {
		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( N_THREADS );

		// We are not using transactions on purpose here, because this approach will cause a context switch
		// and an assertion error if things aren't handled correctly.
		// See Hibernate Reactive issue #2768: https://github.com/hibernate/hibernate-reactive/issues/2768
		vertx
				.deployVerticle( () -> new InsertEntitiesVerticle( (s, entity) -> s
						.persist( entity )
						.thenCompose( v -> s.flush() )
						.thenAccept( v -> s.clear() ) ), deploymentOptions
				)
				.onSuccess( res -> {
					endLatch.waitForEveryone();
					context.completeNow();
				} )
				.onFailure( context::failNow )
				.eventually( () -> vertx.close() );
	}

	@Test
	@DisabledFor(value = DB2, reason = "Exception: IllegalStateException: Needed to have 6 in buffer but only had 0")
	public void testIdentityGeneratorWithTransaction(VertxTestContext context) {
		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( N_THREADS );
		vertx
				.deployVerticle(
						() -> new InsertEntitiesVerticle( (s, entity) -> s
								.withTransaction( t -> s.persist( entity ) ) ), deploymentOptions
				)
				.onSuccess( res -> {
					endLatch.waitForEveryone();
					context.completeNow();
				} )
				.onFailure( context::failNow )
				.eventually( () -> vertx.close() );
	}

	private static class InsertEntitiesVerticle extends AbstractVerticle {

		final BiFunction<Stage.Session, EntityWithGeneratedId, CompletionStage<Void>> insertFun;

		int sequentialOperation = 0;

		public InsertEntitiesVerticle(BiFunction<Stage.Session, EntityWithGeneratedId, CompletionStage<Void>> insertFun) {
			this.insertFun = insertFun;
		}

		@Override
		public void start(Promise<Void> startPromise) {
			startLatch.reached();
			startLatch.waitForEveryone();//Not essential, but to ensure a good level of parallelism
			final String initialThreadName = Thread.currentThread().getName();
			final Stage.Session session = stageSessionFactory.createSession();
			storeMultipleEntities( session )
					.handle( CompletionStages::handle )
					.thenCompose( handler -> session
							.close()
							.thenCompose( handler::getResultAsCompletionStage )
					)
					.whenComplete( (o, throwable) -> {
						endLatch.reached();
						if ( throwable != null ) {
							prettyOut( throwable.getMessage() );
							startPromise.fail( throwable );
						}
						else {
							if ( !initialThreadName.equals( Thread.currentThread().getName() ) ) {
								prettyOut( "Thread switch detected. Expecting " + initialThreadName + ", actual " + Thread.currentThread().getName() );
								startPromise.fail( "Thread switch detected!" );
							}
							else {
								startPromise.complete();
							}
						}
					} );
		}

		private CompletionStage<Void> storeMultipleEntities(Stage.Session s) {
			return loop( 0, ENTITIES_STORED_PER_THREAD, index -> storeEntity( s ) );
		}

		private CompletionStage<Void> storeEntity(Stage.Session s) {
			final Thread beforeOperationThread = Thread.currentThread();
			final int localVerticleOperationSequence = sequentialOperation++;
			final EntityWithGeneratedId entity = new EntityWithGeneratedId();
			entity.name = beforeOperationThread + "__" + localVerticleOperationSequence;
			return insertFun
					.apply( s, entity )
					.thenCompose( v -> beforeOperationThread != Thread.currentThread()
							? failedFuture( new IllegalStateException( "Detected an unexpected switch of carrier threads!" ) )
							: voidFuture() );
		}

		@Override
		public void stop() {
			prettyOut( "Verticle stopped " + super.toString() );
		}
	}

	/**
	 * Trivial entity using default id generation
	 */
	@Entity
	@Table(name = "Entity")
	private static class EntityWithGeneratedId {
		@Id
		@GeneratedValue
		Long id;

		String name;

		public EntityWithGeneratedId() {
		}
	}

	/**
	 * Custom latch which is rather verbose about threads reaching the milestones, to help verifying the design
	 */
	private static final class Latch {
		private final String label;
		private final CountDownLatch countDownLatch;

		public Latch(String label, int membersCount) {
			this.label = label;
			this.countDownLatch = new CountDownLatch( membersCount );
		}

		public void reached() {
			final long count = countDownLatch.getCount();
			countDownLatch.countDown();
			prettyOut( "Reached latch '" + label + "', current countdown is " + ( count - 1 ) );
		}

		public void waitForEveryone() {
			try {
				boolean reachedZero = countDownLatch.await( TIMEOUT_MINUTES, MINUTES );
				if ( reachedZero ) {
					prettyOut( "Everyone has now breached '" + label + "'" );
				}
				else {
					fail( "Time out reached" );
				}
			}
			catch ( InterruptedException e ) {
				fail( e );
			}
		}
	}

	private static void prettyOut(final String message) {
		if ( THREAD_PRETTY_MSG ) {
			final String threadName = Thread.currentThread().getName();
			final long l = System.currentTimeMillis();
			final long seconds = ( l / 1000 ) - initialSecond;
			//We prefix log messages by seconds since bootstrap; I'm preferring this over millisecond precision
			//as it's not very relevant to see exactly how long each stage took (it's actually distracting)
			//but it's more useful to group things coarsely when some lock or timeout introduces a significant
			//divide between some operations (when a starvation or timeout happens it takes some seconds).
			System.out.println( seconds + " - " + threadName + ": " + message );
		}
	}

	private static final long initialSecond = ( System.currentTimeMillis() / 1000 );

}

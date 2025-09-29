/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hibernate.AssertionFailure;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.id.impl.ReactiveGeneratorWrapper;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.session.impl.ReactiveSessionFactoryImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.reactive.vertx.VertxInstance;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.cfg.AvailableSettings.SHOW_SQL;
import static org.hibernate.reactive.BaseReactiveTest.setDefaultProperties;
import static org.hibernate.reactive.provider.Settings.POOL_CONNECT_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * This is a multi-threaded stress test, intentionally consuming some time.
 * The purpose is to verify that the sequence optimizer used by Hibernate Reactive
 * is indeed able to generate unique IDs backed by the database sequences, while
 * running multiple operations in different threads and on multiple Vert.x eventloops.
 * A typical reactive application will not require multiple threads, but we
 * specifically want to test for the case in which the single ID source is being
 * shared across multiple threads and eventloops.
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = MultithreadedIdentityGenerationTest.TIMEOUT_MINUTES, timeUnit = MINUTES)
public class MultithreadedIdentityGenerationTest {

	/*
	 * The number of threads should be higher than the default size of the connection pool so that
	 * this test is also effective in detecting problems with resource starvation.
	 */
	private static final int N_THREADS = 48;
	private static final int IDS_GENERATED_PER_THREAD = 10000;

	//Should finish much sooner, but generating this amount of IDs could be slow on some CIs
	public static final int TIMEOUT_MINUTES = 10;

	// Keeping this disabled because it generates a lot of queries
	private static final boolean LOG_SQL = false;

	/**
	 * If true, it will print info about the threads
	 */
	private static final boolean THREAD_PRETTY_MSG = false;

	private static final Latch startLatch = new Latch( "start", N_THREADS );
	private static final Latch endLatch = new Latch( "end", N_THREADS );

	private static Stage.SessionFactory stageSessionFactory;
	private static Vertx vertx;
	private static SessionFactory sessionFactory;

	@BeforeAll
	public static void setupSessionFactory() {
		final VertxOptions vertxOptions = createVertxOptions();
		vertx = Vertx.vertx( vertxOptions );
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

	private static @NotNull VertxOptions createVertxOptions() {
		final VertxOptions vertxOptions = new VertxOptions();
		vertxOptions.setEventLoopPoolSize( N_THREADS );
		//We relax the blocked thread checks as we'll actually use latches to block them
		//intentionally for the purpose of the test; functionally this isn't required,
		//but it's useful as self-test in the design of this, to ensure that the way
		//things are set up are indeed being run in multiple, separate threads.
		vertxOptions.setBlockedThreadCheckInterval( TIMEOUT_MINUTES );
		vertxOptions.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return vertxOptions;
	}

	@AfterAll
	public static void closeSessionFactory() {
		stageSessionFactory.close();
	}

	private ReactiveGeneratorWrapper getIdGenerator() {
		final ReactiveSessionFactoryImpl hibernateSessionFactory = (ReactiveSessionFactoryImpl) sessionFactory;
		return (ReactiveGeneratorWrapper) hibernateSessionFactory
				.getRuntimeMetamodels()
				.getMappingMetamodel()
				.getEntityDescriptor( "org.hibernate.reactive.MultithreadedIdentityGenerationTest$EntityWithGeneratedId" )
				.getGenerator();
	}

	@Test
	public void testIdentityGenerator(VertxTestContext context) {
		final ReactiveGeneratorWrapper idGenerator = getIdGenerator();
		assertNotNull( idGenerator );

		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( N_THREADS );

		ResultsCollector allResults = new ResultsCollector();

		vertx
				.deployVerticle( () -> new IdGenVerticle( idGenerator, allResults ), deploymentOptions )
				.onSuccess( res -> {
					endLatch.waitForEveryone();
					if ( allResultsAreUnique( allResults ) ) {
						context.completeNow();
					}
					else {
						context.failNow( "Non unique numbers detected" );
					}
				} )
				.onFailure( context::failNow )
				.eventually( () -> vertx.close() );
	}

	private boolean allResultsAreUnique(ResultsCollector allResults) {
		//Add 50 per thread to the total amount of generated ids to allow for gaps
		//in the hi/lo partitioning (not likely to be necessary)
		final int expectedSize = N_THREADS * ( IDS_GENERATED_PER_THREAD + 50 );
		BitSet resultsSeen = new BitSet( expectedSize );
		boolean failed = false;
		for ( List<Long> partialResult : allResults.resultsByThread.values() ) {
			for ( Long aLong : partialResult ) {
				final int intValue = aLong.intValue();
				final boolean existing = resultsSeen.get( intValue );
				if ( existing ) {
					System.out.println( "Duplicate ID detected: " + intValue );
					failed = true;
				}
				resultsSeen.set( intValue );
			}
		}
		return !failed;
	}

	private static class IdGenVerticle extends AbstractVerticle {

		private final ReactiveGeneratorWrapper idGenerator;
		private final ResultsCollector allResults;
		private final ArrayList<Long> generatedIds = new ArrayList<>( IDS_GENERATED_PER_THREAD );

		public IdGenVerticle(ReactiveGeneratorWrapper idGenerator, ResultsCollector allResults) {
			this.idGenerator = idGenerator;
			this.allResults = allResults;
		}

		@Override
		public void start(Promise<Void> startPromise) {
			try {
				startLatch.reached();
				startLatch.waitForEveryone();//Not essential, but to ensure a good level of parallelism
				final String initialThreadName = Thread.currentThread().getName();
				stageSessionFactory
						.withSession( s -> generateMultipleIds( idGenerator, s, generatedIds ) )
						.whenComplete( (o, throwable) -> {
							endLatch.reached();
							if ( throwable != null ) {
								startPromise.fail( throwable );
							}
							else {
								if ( !initialThreadName.equals( Thread.currentThread().getName() ) ) {
									startPromise.fail( "Thread switch detected!" );
								}
								else {
									allResults.deliverResults( generatedIds );
									startPromise.complete();
								}
							}
						} );
			}
			catch (RuntimeException e) {
				startPromise.fail( e );
			}
		}

		@Override
		public void stop() {
			prettyOut( "Verticle stopped " + super.toString() );
		}
	}

	private static class ResultsCollector {

		private final ConcurrentMap<String, List<Long>> resultsByThread = new ConcurrentHashMap<>();

		public void deliverResults(List<Long> generatedIds) {
			final String threadName = Thread.currentThread().getName();
			resultsByThread.put( threadName, generatedIds );
		}
	}

	private static CompletionStage<Void> generateMultipleIds(
			ReactiveGeneratorWrapper idGenerator,
			Stage.Session s,
			ArrayList<Long> collector) {
		return CompletionStages.loop( 0, IDS_GENERATED_PER_THREAD, index -> generateIds( idGenerator, s, collector ) );
	}

	private static CompletionStage<Void> generateIds(
			ReactiveGeneratorWrapper idGenerator,
			Stage.Session s,
			ArrayList<Long> collector) {
		final Thread beforeOperationThread = Thread.currentThread();
		return idGenerator
				.generate( ( (StageSessionImpl) s ).unwrap( ReactiveConnectionSupplier.class ), new EntityWithGeneratedId() )
				.thenAccept( o -> {
					if ( beforeOperationThread != Thread.currentThread() ) {
						throw new IllegalStateException( "Detected an unexpected switch of carrier threads!" );
					}
					collector.add( (Long) o );
				} );
	}

	/**
	 * Trivial entity using a Sequence for Id generation
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
				if ( !countDownLatch.await( TIMEOUT_MINUTES, MINUTES ) ) {
					throw new AssertionFailure( "Time out reached!" );
				}
				prettyOut( "Everyone has now breached '" + label + "'" );
			}
			catch (InterruptedException e) {
				e.printStackTrace();
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

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.stage.Stage;
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
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.cfg.AvailableSettings.SHOW_SQL;
import static org.hibernate.reactive.BaseReactiveTest.setDefaultProperties;
import static org.hibernate.reactive.util.internal.CompletionStages.loop;
import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Reproducer for HR000069 thread-affinity error during concurrent find-then-delete
 * operations across multiple Vert.x event-loop threads.
 * <p>
 * When multiple sessions perform {@code session.find()} followed by
 * {@code session.remove()} and {@code session.flush()} concurrently,
 * the SQL response from the DELETE may complete on a different event-loop
 * thread than the one that opened the session, triggering the HR000069
 * thread-affinity check inside {@code ReactiveDeleteCoordinatorStandard}.
 *
 * @see <a href="https://github.com/quarkusio/quarkus/issues/41945">quarkusio/quarkus#41945</a>
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = 5, timeUnit = MINUTES)
public class MultithreadedFindAndDeleteTest {

	private static final int N_THREADS = 8;
	private static final int ENTITIES_PER_THREAD = 20;
	private static final int TOTAL_ENTITIES = N_THREADS * ENTITIES_PER_THREAD;
	private static final int TIMEOUT_MINUTES = 5;

	private static Stage.SessionFactory stageSessionFactory;
	private static Vertx vertx;

	@BeforeAll
	public static void setupSessionFactory() throws Exception {
		vertx = Vertx.vertx( getVertxOptions() );
		Configuration configuration = new Configuration();
		setDefaultProperties( configuration );
		configuration.addAnnotatedClass( Fruit.class );
		configuration.setProperty( SHOW_SQL, "false" );
		configuration.setProperty( AvailableSettings.POOL_SIZE, String.valueOf( N_THREADS + 2 ) );
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() )
				.addService( VertxInstance.class, () -> vertx );
		StandardServiceRegistry registry = builder.build();
		SessionFactory sessionFactory = configuration.buildSessionFactory( registry );
		stageSessionFactory = sessionFactory.unwrap( Stage.SessionFactory.class );

		CountDownLatch insertLatch = new CountDownLatch( 1 );
		AtomicReference<Throwable> insertError = new AtomicReference<>();
		stageSessionFactory
				.withSession( s -> loop(
						0, TOTAL_ENTITIES,
						i -> s.persist( new Fruit( i, "fruit-" + i ) )
				).thenCompose( v -> s.flush() ) )
				.whenComplete( (v, t) -> {
					if ( t != null ) {
						insertError.set( t );
					}
					insertLatch.countDown();
				} );
		if ( !insertLatch.await( TIMEOUT_MINUTES, MINUTES ) ) {
			throw new RuntimeException( "Timed out inserting test entities" );
		}
		if ( insertError.get() != null ) {
			throw new RuntimeException( "Failed inserting test entities", insertError.get() );
		}
	}

	private static VertxOptions getVertxOptions() {
		final VertxOptions vertxOptions = new VertxOptions();
		vertxOptions.setEventLoopPoolSize( N_THREADS );
		vertxOptions.setBlockedThreadCheckInterval( TIMEOUT_MINUTES );
		vertxOptions.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return vertxOptions;
	}

	@AfterAll
	public static void closeSessionFactory() {
		stageSessionFactory.close();
	}

	@Test
	public void testConcurrentFindAndDelete(VertxTestContext context) {
		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( N_THREADS );

		final AtomicInteger idSequence = new AtomicInteger( 0 );

		vertx
				.deployVerticle(
						() -> new FindAndDeleteVerticle( idSequence ),
						deploymentOptions
				)
				.onSuccess( res -> context.completeNow() )
				.onFailure( context::failNow )
				.eventually( () -> vertx.close() );
	}

	private static class FindAndDeleteVerticle extends AbstractVerticle {
		private final AtomicInteger idSequence;

		FindAndDeleteVerticle(AtomicInteger idSequence) {
			this.idSequence = idSequence;
		}

		@Override
		public void start(Promise<Void> startPromise) {
			final int startId = idSequence.getAndAdd( ENTITIES_PER_THREAD );

			stageSessionFactory
					.withSession( session -> loop(
							0, ENTITIES_PER_THREAD,
							i -> session
									.find( Fruit.class, startId + i )
									.thenCompose( entity -> {
										if ( entity != null ) {
											return session.remove( entity )
													.thenCompose( v -> session.flush() )
													.thenAccept( v -> session.clear() );
										}
										return voidFuture();
									} )
					) )
					.whenComplete( (v, throwable) -> {
						if ( throwable != null ) {
							startPromise.fail( throwable );
						}
						else {
							startPromise.complete();
						}
					} );
		}
	}

	@Entity(name = "Fruit41945")
	@Table(name = "fruit_41945")
	public static class Fruit {
		@Id
		private Integer id;

		private String name;

		public Fruit() {
		}

		public Fruit(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}
}

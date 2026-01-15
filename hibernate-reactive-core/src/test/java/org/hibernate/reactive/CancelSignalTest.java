/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Metrics;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * This test is supposed to check that when the uni is canceled there are no open connection left.
 * But, it seems more complicated than necessary, and it causes several errors on the log because the session ends up
 * in different threads. We need to update it in the future.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class CancelSignalTest extends BaseReactiveTest {
	private static final String CANCELED = "Canceled!";
	private static final int EXECUTION_SIZE = 10;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		// We don't need to delete anything, keep the log cleaner
		return voidFuture();
	}

	@Test
	public void cleanupConnectionWhenCancelSignal(VertxTestContext context) {
		// larger than 'sql pool size' to check entering the 'pool waiting queue'
		final CountDownLatch firstSessionWaiter = new CountDownLatch( 1 );
		final Queue<Cancellable> cancellableQueue = new ConcurrentLinkedQueue<>();
		final List<CompletableFuture<?>> allFutures = new ArrayList<>();

		final String[] results = new String[EXECUTION_SIZE];
		ExecutorService withSessionExecutor = Executors.newFixedThreadPool( EXECUTION_SIZE );
		for ( int j = 0; j < EXECUTION_SIZE; j++ ) {
			final int i = j;
			allFutures.add( runAsync( () -> {
						  CountDownLatch countDownLatch = new CountDownLatch( 1 );
						  Cancellable cancellable = getMutinySessionFactory()
								  .withSession( s -> {
									  firstSessionWaiter.countDown();
									  return s.find( GuineaPig.class, 1 )
											  .invoke( () -> assertThat( sqlPendingMetric() ).isEqualTo( 1.0 ) )
											  .onItem().delayIt().by( Duration.of( 500, ChronoUnit.MILLIS ) );
								  } )
								  // Keep track that the cancellation occurred
								  .onCancellation().invoke( () -> results[i] = CANCELED )
								  // CountDownLatch should be called in any case
								  .onTermination().invoke( countDownLatch::countDown )
								  .subscribe()
								  // We are canceling the job, it shouldn't reach this point
								  .with( ignore -> context
										  .failNow( "withSession operation has not been canceled" )
								  );
						  cancellableQueue.add( cancellable );
						  await( countDownLatch );
					  },
					  withSessionExecutor
			) );
		}

		ExecutorService cancelExecutor = Executors.newFixedThreadPool( EXECUTION_SIZE );
		for ( int i = 0; i < EXECUTION_SIZE; i++ ) {
			allFutures.add( runAsync( () -> {
						  await( firstSessionWaiter );
						  cancellableQueue.poll().cancel();
						  sleep( 500 );
					  },
					  cancelExecutor
			) );
		}

		test(
				context, allOf( allFutures.toArray( new CompletableFuture<?>[0] ) )
						.thenAccept( x -> {
							assertThat( results )
									.as( "Some jobs have not been canceled" )
									.containsOnly( CANCELED );
							assertThat( sqlPendingMetric() ).isEqualTo( 0.0 );
						} )
		);
	}

	private static double sqlPendingMetric() {
		return Metrics.globalRegistry.find( "vertx.sql.processing.pending" )
				.gauge()
				.value();
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		}
		catch (InterruptedException e) {
			throw new RuntimeException( e );
		}
	}

	private static void sleep(int millis) {
		try {
			// Add sleep to create a test that delays processing
			Thread.sleep( millis );
		}
		catch (InterruptedException e) {
			throw new RuntimeException( e );
		}
	}

	@Entity
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}

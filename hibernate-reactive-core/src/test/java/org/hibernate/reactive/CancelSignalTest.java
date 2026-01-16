/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.jboss.logging.Logger;

import io.micrometer.core.instrument.Metrics;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.Arrays.stream;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * The test fails occasionally because the find returns before the cancel operation is executed.
 * It also logs a lot of errors because the session is opened in a thread and closed in another.
 * I couldn't find a solution that works consistently, so I'm disabling it for now.
 */
@Disabled
public class CancelSignalTest extends BaseReactiveTest {
	private static final Logger LOG = Logger.getLogger( CancelSignalTest.class );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Test
	public void cleanupConnectionWhenCancelSignal(VertxTestContext context) {
		// larger than 'sql pool size' to check entering the 'pool waiting queue'
		int executeSize = 10;
		CountDownLatch firstSessionWaiter = new CountDownLatch( 1 );
		Queue<Cancellable> cancellableQueue = new ConcurrentLinkedQueue<>();

		ExecutorService withSessionExecutor = Executors.newFixedThreadPool( executeSize );
		// Create some jobs that are going to be cancelled asynchronously
		CompletableFuture[] withSessionFutures = IntStream
				.range( 0, executeSize )
				.mapToObj( i -> runAsync(
						() -> {
							CountDownLatch countDownLatch = new CountDownLatch( 1 );
							Cancellable cancellable = getMutinySessionFactory()
									.withSession( s -> {
										LOG.debug( "start withSession: " + i );
										sleep( 100 );
										firstSessionWaiter.countDown();
										return s.find( GuineaPig.class, 1 );
									} )
									.onTermination().invoke( () -> {
										countDownLatch.countDown();
										LOG.debug( "future " + i + " terminated" );
									} )
									.subscribe().with( item -> LOG.debug( "end withSession: "  + i  ) );
							cancellableQueue.add( cancellable );
							await( countDownLatch );
						},
						withSessionExecutor
				) )
				.toArray( CompletableFuture[]::new );

		// Create jobs that are going to cancel the previous ones
		ExecutorService cancelExecutor = Executors.newFixedThreadPool( executeSize );
		CompletableFuture[] cancelFutures = IntStream
				.range( 0, executeSize )
				.mapToObj( i -> runAsync(
						() -> {
							await( firstSessionWaiter );
							cancellableQueue.poll().cancel();
							sleep( 500 );
						},
						cancelExecutor
				) )
				.toArray( CompletableFuture[]::new );

		CompletableFuture<Void> allFutures = allOf( concat( stream( withSessionFutures ), stream( cancelFutures ) )
						.toArray( CompletableFuture[]::new )
		);

		// Test that there shouldn't be any pending process
		test( context, allFutures.thenAccept( x -> assertThat( sqlPendingMetric() ).isEqualTo( 0.0 ) ) );
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

	@Entity(name = "GuineaPig")
	@Table(name = "Pig")
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

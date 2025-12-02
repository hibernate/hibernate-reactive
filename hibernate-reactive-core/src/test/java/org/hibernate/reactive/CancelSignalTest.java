/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

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

import org.jboss.logging.Logger;

import io.micrometer.core.instrument.Metrics;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class CancelSignalTest extends BaseReactiveTest {
	private static final Logger LOG = Logger.getLogger( CancelSignalTest.class );

	private static final int EXECUTION_SIZE = 10;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		// We don't need to delete anything
		return voidFuture();
	}

	@Test
	public void cleanupConnectionWhenCancelSignal(VertxTestContext context) {
		// larger than 'sql pool size' to check entering the 'pool waiting queue'
		CountDownLatch firstSessionWaiter = new CountDownLatch( 1 );
		Queue<Cancellable> cancellableQueue = new ConcurrentLinkedQueue<>();

		final List<CompletableFuture<?>> allFutures = new ArrayList<>();

		ExecutorService withSessionExecutor = Executors.newFixedThreadPool( EXECUTION_SIZE );
		for ( int j = 0; j < EXECUTION_SIZE; j++ ) {
			final int i = j;
			allFutures.add( runAsync( () -> {
						  CountDownLatch countDownLatch = new CountDownLatch( 1 );
						  Cancellable cancellable = getMutinySessionFactory()
								  .withSession( s -> {
									  LOG.info( "start withSession: " + i );
									  sleep( 100 );
									  firstSessionWaiter.countDown();
									  return s.find( GuineaPig.class, 1 );
								  } )
								  .onCancellation().invoke( () -> {
									  LOG.info( "future " + i + " cancelled" );
									  countDownLatch.countDown();
								  } )
								  .subscribe()
								  // We cancelled the job, it shouldn't really finish
								  .with( item -> LOG.info( "end withSession: "  + i  ) );
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
						.thenAccept( x -> assertThat( sqlPendingMetric() ).isEqualTo( 0.0 ) )
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

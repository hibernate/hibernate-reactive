/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.net.impl.pool.CombinerExecutor;
import io.vertx.core.net.impl.pool.Executor;
import io.vertx.core.net.impl.pool.Task;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;

/**
 * A {@link ReactiveIdentifierGenerator} which uses the database to allocate
 * blocks of ids. A block is identified by its "hi" value (the first id in
 * the block). While a new block is being allocated, concurrent streams will
 * defer the operation without blocking. A request for a new block serves at most
 * {@code blockSize} waiting streams; further streams trigger another request, and
 * the ids of a block that are not needed by its waiters are kept for later use.
 *
 * @author Gavin King
 * @author Davide D'Alto
 * @author Sanne Grinovero
 */
public abstract class BlockingIdentifierGenerator implements ReactiveIdentifierGenerator<Long> {

	private static final Log LOG = make( Log.class, lookup() );

	/**
	 * The block size (the number of "lo" values for each "hi" value)
	 */
	protected abstract int getBlockSize();

	private final GeneratorState state = new GeneratorState();

	//Access to the critical section is to be performed exclusively
	//via an Action passed to this executor, to ensure exclusive
	//modification access.
	//This replaces the synchronization blocks one would see in a similar
	//service in Hibernate ORM, but using a non-blocking cooperative design.
	private final CombinerExecutor<GeneratorState> executor = new CombinerExecutor<>( state );

	/**
	 * Allocate a new block, by obtaining the next "hi" value from the database
	 */
	protected abstract CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session);

	@Override
	public Object generate(SharedSessionContractImplementor session, Object object) {
		throw LOG.nonReactiveMethodCall( "generate(ReactiveConnectionSupplier, Object)" );
	}

	@Override
	public Long generate(SharedSessionContractImplementor session, Object owner, Object currentValue, EventType eventType) {
		throw LOG.nonReactiveMethodCall( "generate(ReactiveConnectionSupplier, Object, Object, EventType)" );
	}

	//Not strictly necessary to put these fields into a dedicated class, but it helps
	//to reason about what the current state is and what the CombinerExecutor is
	//supposed to work on.
	private static class GeneratorState {
		//The block currently being consumed: the ids in [nextId, endId) are still available
		private long nextId;
		private long endId;
		//The fetch new callers can still join, or null if there isn't one.
		//Invariant: it is in flight, has fewer than blockSize waiters and is not completed.
		//Only accessed via the CombinerExecutor.
		private Fetch openFetch;
	}

	//Critical section: needs to be accessed exclusively via the CombinerExecutor
	//when there's contention; direct invocation is allowed in the fast path.
	private synchronized long next() {
		return state.nextId < state.endId
				? state.nextId++
				: -1; //flag value indicating that we need to hit db
	}

	//Critical section: needs to be accessed exclusively via the CombinerExecutor.
	//Uses the ids of the block [hi, hi + blockSize) that were not handed out to waiters.
	//Only the fetch that was still open can have unused ids, and a fetch is only opened
	//when the current block is exhausted: no other fetch can refill it while it's open.
	private synchronized void useUnusedIds(long hi, int used) {
		if ( used < getBlockSize() ) {
			state.nextId = hi + used;
			state.endId = hi + getBlockSize();
		}
	}

	/**
	 * A request for a new block that is in flight: it serves at most {@code blockSize} waiters,
	 * so that each of them gets an id after a single round-trip to the database.
	 */
	private final class Fetch {
		//The first waiter started the fetch, and the request uses its connection
		private final List<GenerateIdAction> waiters = new ArrayList<>();

		private void complete(long hi) {
			useUnusedIds( hi, waiters.size() );
			for ( int i = 0; i < waiters.size(); i++ ) {
				waiters.get( i ).result.complete( hi + i );
			}
		}

		private void fail(GeneratorState state, Throwable throwable) {
			// The failure might come from the connection of the first waiter (an aborted transaction,
			// for example), so the other waiters try again: one of them starts a new fetch with its own connection
			waiters.get( 0 ).result.completeExceptionally( throwable );
			for ( int i = 1; i < waiters.size(); i++ ) {
				waiters.get( i ).execute( state );
			}
		}
	}

	@Override
	public CompletionStage<Long> generate(ReactiveConnectionSupplier connectionSupplier, Object ignored) {
		Objects.requireNonNull( connectionSupplier );

		//Before submitting a task to the executor, let's try our luck via the fast-path
		//(this does actually hit a synchronization, but it's extremely short)
		final long next = next();
		if ( next != -1 ) {
			return completedFuture( next );
		}

		//Another special case we need to deal with; this is an unlikely configuration, but
		//if it were to happen we should be better off with direct execution rather than using
		//the co-operative executor:
		if ( getBlockSize() <= 1 ) {
			return nextHiValue( connectionSupplier );
		}

		final CompletableFuture<Long> resultForThisEventLoop = new CompletableFuture<>();
		// We use supplyStage so that, no matter if there's an exception, we always return something that will complete
		return supplyStage( () -> {
			final CompletableFuture<Long> result = new CompletableFuture<>();
			final Context context = Vertx.currentContext();
			executor.submit( new GenerateIdAction( connectionSupplier, result ) );
			result.whenComplete( (id, t) -> {
				final Context newContext = Vertx.currentContext();
				//Need to be careful in resuming processing on the same context as the original
				//request, potentially having to switch back if we're no longer executing on the same:
				if ( newContext != context ) {
					context.runOnContext( v -> complete( resultForThisEventLoop, id, t ) );
				}
				else {
					complete( resultForThisEventLoop, id, t );
				}
			} );
			return resultForThisEventLoop;
		} );
	}

	private final class GenerateIdAction implements Executor.Action<GeneratorState> {

		private final ReactiveConnectionSupplier connectionSupplier;
		private final CompletableFuture<Long> result;
		private final ContextInternal creationContext;

		public GenerateIdAction(ReactiveConnectionSupplier connectionSupplier, CompletableFuture<Long> result) {
			this.creationContext = ContextInternal.current();
			this.connectionSupplier = Objects.requireNonNull( connectionSupplier );
			this.result = Objects.requireNonNull( result );
		}

		@Override
		public Task execute(GeneratorState state) {
			long local = next();
			if ( local >= 0 ) {
				// We don't need to hit the database: the block still has ids
				result.complete( local );
				return null;
			}
			// Join the open fetch if there is one, otherwise start a new one. A fetch that
			// reaches blockSize waiters is closed so that the next caller starts another one.
			if ( state.openFetch == null ) {
				final Fetch fetch = new Fetch();
				try {
					creationContext.runOnContext( v -> fetchHiValue( fetch ) );
				}
				catch (Throwable e) {
					// The fetch didn't start, so no other caller must join it
					result.completeExceptionally( e );
					return null;
				}
				state.openFetch = fetch;
			}
			state.openFetch.waiters.add( this );
			if ( state.openFetch.waiters.size() >= getBlockSize() ) {
				state.openFetch = null;
			}
			return null;
		}

		private void fetchHiValue(Fetch fetch) {
			CompletionStage<Long> stage;
			try {
				stage = nextHiValue( connectionSupplier );
			}
			catch (Throwable e) {
				// nextHiValue() could throw an exception before returning a completion stage
				stage = failedFuture( e );
			}
			stage.whenComplete( (newlyGeneratedHi, throwable) -> executor.submit( generatorState -> {
				// No new caller can join a fetch once it's completed, even if it failed
				if ( generatorState.openFetch == fetch ) {
					generatorState.openFetch = null;
				}
				if ( throwable != null ) {
					fetch.fail( generatorState, throwable );
				}
				else {
					fetch.complete( newlyGeneratedHi );
				}
				return null;
			} ) );
		}
	}

	private static <T> void complete(CompletableFuture<T> future, final T result, final Throwable throwable) {
		if ( throwable != null ) {
			future.completeExceptionally( throwable );
		}
		else {
			future.complete( result );
		}
	}
}

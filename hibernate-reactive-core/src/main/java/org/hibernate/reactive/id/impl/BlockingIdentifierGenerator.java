/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.net.impl.pool.CombinerExecutor;
import io.vertx.core.net.impl.pool.Executor;
import io.vertx.core.net.impl.pool.Task;

import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A {@link ReactiveIdentifierGenerator} which uses the database to allocate
 * blocks of ids. A block is identified by its "hi" value (the first id in
 * the block). While a new block is being allocated, concurrent streams will
 * defer the operation without blocking.
 *
 * @author Gavin King
 * @author Davide D'Alto
 * @author Sanne Grinovero
 */
public abstract class BlockingIdentifierGenerator implements ReactiveIdentifierGenerator<Long> {

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
	private final CombinerExecutor executor = new CombinerExecutor( state );

	/**
	 * Allocate a new block, by obtaining the next "hi" value from the database
	 */
	protected abstract CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session);

	//Not strictly necessary to put these fields into a dedicated class, but it help
	//to reason about what the current state is and what the CombinerExecutor is
	//supposed to work on.
	private static class GeneratorState {
		private int loValue;
		private long hiValue;
	}

	//Critical section: needs to be accessed exclusively via the CombinerExecutor
	//when there's contention; direct invocation is allowed in the fast path.
	private synchronized long next() {
		return state.loValue > 0 && state.loValue < getBlockSize()
				? state.hiValue + state.loValue++
				: -1; //flag value indicating that we need to hit db
	}

	//Critical section: needs to be accessed exclusively via the CombinerExecutor
	private synchronized long next(long hi) {
		state.hiValue = hi;
		state.loValue = 1;
		return hi;
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
			return nextHiValue( connectionSupplier ).thenApply( this::next );
		}

		final CompletableFuture<Long> resultForThisEventLoop = new CompletableFuture<>();
		final CompletableFuture<Long> result = new CompletableFuture<>();
		final Context context = Vertx.currentContext();
		executor.submit( new GenerateIdAction( connectionSupplier, result ) );
		result.whenComplete( (id, t) -> {
			final Context newContext = Vertx.currentContext();
			//Need to be careful in resuming processing on the same context as the original
			//request, potentially having to switch back if we're no longer executing on the same:
			if ( newContext != context ) {
				if ( t != null ) {
					context.runOnContext( v -> resultForThisEventLoop.completeExceptionally( t ) );
				}
				else {
					context.runOnContext( v -> resultForThisEventLoop.complete( id ) );
				}
			}
			else {
				if ( t != null ) {
					resultForThisEventLoop.completeExceptionally( t );
				}
				else {
					resultForThisEventLoop.complete( id );
				}
			}
		} );
		return resultForThisEventLoop;
	}

	private final class GenerateIdAction implements Executor.Action<GeneratorState> {

		private final ReactiveConnectionSupplier connectionSupplier;
		private final CompletableFuture<Long> result;

		public GenerateIdAction(ReactiveConnectionSupplier connectionSupplier, CompletableFuture<Long> result) {
			this.connectionSupplier = Objects.requireNonNull( connectionSupplier );
			this.result = Objects.requireNonNull( result );
		}

		@Override
		public Task execute(GeneratorState state) {
			long local = next();
			if ( local >= 0 ) {
				// We don't need to update or initialize the hi
				// value in the table, so just increment the lo
				// value and return the next id in the block
				completedFuture( local ).whenComplete( this::acceptAsReturnValue );
				return null;
			}
			else {
				nextHiValue( connectionSupplier )
						.whenComplete( (newlyGeneratedHi, throwable) -> {
							if ( throwable != null ) {
								result.completeExceptionally( throwable );
							}
							else {
								//We ignore the state argument as we actually use the field directly
								//for convenience, but they are the same object.
								executor.submit( stateIgnored -> {
									result.complete( next( newlyGeneratedHi ) );
									return null;
								} );
							}
						} );
				return null;
			}
		}

		private void acceptAsReturnValue(final Long aLong, final Throwable throwable) {
			if ( throwable != null ) {
				result.completeExceptionally( throwable );
			}
			else {
				result.complete( aLong );
			}
		}
	}

}

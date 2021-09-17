/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.async.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hibernate.reactive.util.impl.CompletionStages;

/**
 * Copy of com.ibm.asyncutil.iteration.AyncTrampoline from com.ibm.async:asyncutil:0.1.0
 * without all the methods and imports we don't need for Hibernate Reactive.
 * <p>
 * Static methods for asynchronous looping procedures without exhausting the stack.
 *
 * <p>
 * When working with {@link CompletionStage}, it's often desirable to have a loop like construct
 * which keeps producing stages until some condition is met. Because continuations are asynchronous,
 * it's usually easiest to do this with a recursive approach:
 *
 * <pre>
 * {@code
 * CompletionStage<Integer> getNextNumber();
 *
 * CompletionStage<Integer> getFirstOddNumber(int current) {
 *   if (current % 2 != 0)
 *     // found odd number
 *     return CompletableFuture.completedFuture(current);
 *   else
 *     // get the next number and recurse
 *     return getNextNumber().thenCompose(next -> getFirstOddNumber(next));
 * }
 * }
 * </pre>
 * <p>
 * The problem with this is that if the implementation of getNextNumber happens to be synchronous
 *
 * <pre>
 * {@code
 * CompletionStage<Integer> getNextNumber() {
 *   return CompletableFuture.completedFuture(random.nextInt());
 * }
 * }
 * </pre>
 * <p>
 * then getFirstOddNumber can easily cause a stack overflow. This situation often happens when a
 * cache is put under an async API, and all the values are cached and returned immediately. This
 * could be avoided by scheduling the recursive calls back to a thread pool using
 * {@link CompletionStage#thenComposeAsync}, however the overhead of the thread pool submissions may
 * be high and may cause unnecessary context switching.
 *
 * <p>
 * The methods on this class ensure that the stack doesn't blow up - if multiple calls happen on the
 * same thread they are queued and run in a loop. You could write the previous example like this:
 *
 * <pre>
 * {@code
 * CompletionStage<Integer> getFirstOddNumber(int initial) {
 *   return AsyncTrampoline.asyncWhile(
 *    i -> i % 2 == 0,
 *    i -> getNextNumber(),
 *    initial);
 * }
 * }
 * </pre>
 * <p>
 * Though this class provides efficient methods for a few loop patterns, many are better represented
 * by the more expressive API available on {@link AsyncIterator}, which is also stack safe. For
 * example, the preceding snippet can be expressed as {@code
 * AsyncIterator.generate(this::getNextNumber).find(i -> i % 2 != 0)}
 *
 * @see AsyncIterator
 */
public final class AsyncTrampoline {

	private AsyncTrampoline() {
	}

	private static class TrampolineInternal<T> extends CompletableFuture<T> {

		private final Predicate<? super T> shouldContinue;
		private final Function<? super T, ? extends CompletionStage<T>> f;

		private TrampolineInternal(
				final Predicate<? super T> shouldContinue,
				final Function<? super T, ? extends CompletionStage<T>> f) {
			this.shouldContinue = shouldContinue;
			this.f = f;
		}

		private static <T> CompletionStage<T> trampoline(
				final Predicate<? super T> shouldContinue,
				final Function<? super T, ? extends CompletionStage<T>> f,
				final T initialValue) {
			final TrampolineInternal<T> trampolineInternal = new TrampolineInternal<>( shouldContinue, f );
			trampolineInternal.unroll( initialValue, null, null );
			return trampolineInternal;
		}

		private void unroll(
				final T completed, final Thread previousThread, final PassBack<T> previousPassBack) {
			final Thread currentThread = Thread.currentThread();

			// we need to track termination in case f queues the future and currentThread later picks it
			// up, leading to sequential runs from the same thread but different stacks
			if ( currentThread.equals( previousThread ) && ( previousPassBack != null && previousPassBack.isRunning ) ) {
				previousPassBack.item = completed;
			}
			else {
				final PassBack<T> currentPassBack = new PassBack<>();
				T c = completed;
				do {
					try {
						if ( this.shouldContinue.test( c ) ) {
							this.f.apply( c ).whenComplete( (next, ex) -> {
								if ( ex != null ) {
									completeExceptionally( ex );
								}
								else {
									unroll( next, currentThread, currentPassBack );
								}
							} );
						}
						else {
							complete( c );
							return;
						}
					}
					catch (final Throwable e) {
						completeExceptionally( e );
						return;
					}
				} while ( ( c = currentPassBack.poll() ) != PassBack.NIL );
				currentPassBack.isRunning = false;
			}
		}

		@SuppressWarnings("unchecked")
		private static class PassBack<T> {
			private static final Object NIL = new Object();

			boolean isRunning = true;
			T item = (T) NIL;

			T poll() {
				final T c = this.item;
				this.item = (T) NIL;
				return c;
			}
		}
	}

	/**
	 * Repeatedly applies an asynchronous function {@code fn} to a value until {@code shouldContinue}
	 * returns {@code false}. The asynchronous equivalent of
	 *
	 * <pre>
	 * {@code
	 * T loop(Predicate shouldContinue, Function fn, T initialValue) {
	 *   T t = initialValue;
	 *   while (shouldContinue.test(t)) {
	 *     t = fn.apply(t);
	 *   }
	 *   return t;
	 * }
	 * }
	 * </pre>
	 *
	 * <p>
	 * Effectively produces {@code fn(seed).thenCompose(fn).thenCompose(fn)... .thenCompose(fn)} until
	 * an value fails the predicate. Note that predicate will be applied on seed (like a while loop,
	 * the initial value is tested). If the predicate or fn throw an exception,
	 * or the {@link CompletionStage} returned by fn completes exceptionally, iteration will stop and
	 * an exceptional stage will be returned.
	 *
	 * @param shouldContinue a predicate which will be applied to every intermediate T value
	 * (including the {@code initialValue}) until it fails and looping stops.
	 * @param fn the function for the loop body which produces a new {@link CompletionStage} based on
	 * the result of the previous iteration.
	 * @param initialValue the value that will initially be passed to {@code fn}, it will also be
	 * initially tested by {@code shouldContinue}
	 * @param <T> the type of elements produced by the loop
	 *
	 * @return a {@link CompletionStage} that completes with the first value t such that {@code
	 * shouldContinue.test(T) == false}, or with an exception if one was thrown.
	 */
	public static <T> CompletionStage<T> asyncWhile(
			final Predicate<? super T> shouldContinue,
			final Function<? super T, ? extends CompletionStage<T>> fn,
			final T initialValue) {
		return TrampolineInternal.trampoline( shouldContinue, fn, initialValue );
	}

	/**
	 * Repeatedly uses the function {@code fn} to produce a {@link CompletionStage} of a boolean,
	 * stopping when then boolean is {@code false}. The asynchronous equivalent of {@code
	 * while(fn.get());}. Generally, the function fn must perform some side effect for this method to
	 * be useful. If the {@code fn} throws or produces an exceptional {@link CompletionStage}, an
	 * exceptional stage will be returned.
	 *
	 * @param fn a {@link Supplier} of a {@link CompletionStage} that indicates whether iteration
	 * should continue
	 *
	 * @return a {@link CompletionStage} that is complete when a stage produced by {@code fn} has
	 * returned {@code false}, or with an exception if one was thrown
	 */
	public static CompletionStage<Void> asyncWhile(
			final Supplier<? extends CompletionStage<Boolean>> fn) {
		return AsyncTrampoline.asyncWhile( b -> b, b -> fn.get(), true )
				.thenCompose( CompletionStages::voidFuture );
	}
}

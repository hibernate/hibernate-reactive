/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.async.impl;

import org.hibernate.reactive.engine.impl.InternalStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

/**
 * Copy of com.ibm.asyncutil.iteration.AsyncIterators from com.ibm.async:asyncutil:0.1.0
 * without all the methods and imports we don't need for Hibernate Reactive.
 * <p>
 * Package private methods to use in {@link AsyncIterator}
 */
class AsyncIterators {

	private AsyncIterators() {
	}

	static final EmptyAsyncIterator<?> EMPTY_ITERATOR = new EmptyAsyncIterator<>();

	private static class EmptyAsyncIterator<T> implements AsyncIterator<T> {

		@Override
		public InternalStage<Either<End, T>> nextStage() {
			return End.endStage();
		}

		@Override
		public String toString() {
			return "EmptyAsyncIterator";
		}
	}

	@SuppressWarnings("unchecked")
	static <A, R> R finishContainer(final A accumulator, final Collector<?, A, R> collector) {
		// cast instead of applying the finishing function if the collector indicates the
		// finishing function is just identity
		return collector.characteristics().contains( Collector.Characteristics.IDENTITY_FINISH )
				? ( (R) accumulator )
				: collector.finisher().apply( accumulator );
	}

	static <T> InternalStage<T> convertSynchronousException(
			final Supplier<? extends InternalStage<T>> supplier) {
		try {
			return supplier.get();
		}
		catch (final Throwable e) {
			return failedFuture( e );
		}
	}

	static <T, U> AsyncIterator<U> thenApplyImpl(
			final AsyncIterator<T> it,
			final Function<? super T, ? extends U> f) {
		return new AsyncIterator<U>() {
			@Override
			public InternalStage<Either<End, U>> nextStage() {
				final InternalStage<Either<End, T>> next = it.nextStage();

				return next.thenApply( this::eitherFunction );
			}

			Either<End, U> eitherFunction(final Either<End, T> either) {
				return either.map( f );
			}

			@Override
			public InternalStage<Void> close() {
				return it.close();
			}
		};
	}

	static <T, U> AsyncIterator<U> thenComposeImpl(
			final AsyncIterator<T> it,
			final Function<? super T, ? extends InternalStage<U>> f) {

		return new AsyncIterator<U>() {
			@Override
			public InternalStage<Either<End, U>> nextStage() {
				final InternalStage<Either<End, T>> nxt = it.nextStage();
				return nxt.thenCompose( this::eitherFunction );
			}

			/*
			 * if there's a value, apply f and wrap the result in an Either, otherwise just return end
			 * marker
			 */
			private InternalStage<Either<End, U>> eitherFunction(final Either<End, T> either) {
				return either.fold(
						end -> End.endStage(),
						t -> f.apply( t ).thenApply( Either::right )
				);
			}

			@Override
			public InternalStage<Void> close() {
				return it.close();
			}
		};
	}
}

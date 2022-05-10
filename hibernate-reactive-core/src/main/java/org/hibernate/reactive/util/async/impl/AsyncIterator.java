/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.async.impl;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * Copy of com.ibm.asyncutil.iteration.AsyncIterator from com.ibm.async:asyncutil:0.1.0
 * without all the methods and imports we don't need for Hibernate Reactive.
 * <p>
 * A mechanism for asynchronously generating and consuming values
 *
 * <p>Consider this an async version of {@link Stream}.
 *
 * <p>AsyncIterators have lazy, pull based, evaluation semantics - values are not computed until
 * they are needed. AsyncIterators are not immutable - like streams, each value they produce is
 * consumed only once. Typically you should not apply multiple transformations to the same source
 * AsyncIterator, it almost certainly won't do what you want it to do.
 *
 * <p>Implementors of the interface need only implement {@link #nextStage()}.
 *
 * <p>A note on thread safety: This class makes no assumption that {@link #nextStage()} is thread
 * safe! Many methods that generate transformed iterators assume that nextStage will not be called
 * concurrently, and even stronger, that nextStage won't be called again until the previous stage
 * returned by nextStage has completed.
 *
 * <p>Parallelization may still be accomplished using the <i>partially eager</i> methods described
 * below. The difference is that the parallelization in that case is from <b>producing</b> values in
 * parallel, <b>not consuming</b> values in parallel.
 *
 * <p>To implement an AsyncIterator you must only implement the {@link #nextStage()} method-
 * however, it is recommended that users avoid actually using nextStage to consume the results of
 * iteration. It is less expressive and it can also be error prone; it is easy to cause a stack
 * overflow by incorrectly recursing on calls to nextStage. You should prefer to use the other
 * higher level methods on this interface.
 *
 * <p>There are 2 main categories of such methods on this interface: Intermediate and Terminal.
 * These methods can be combined to form pipelines, which generally consist of a source (often
 * created with the static constructor methods on this interface ({@link #fromIterator(Iterator)},
 * etc)), followed by zero or more intermediate operations (such
 * as {@link #filter(Predicate)}, {@link #thenApply(Function)}), and completed with a terminal
 * operation which returns a {@link CompletionStage} (such as {@link #forEach(Consumer)}).
 * For example, suppose we wanted to accomplish the following
 * (blocking) procedure:
 *
 * <pre>{@code
 * // request and lookup records one by one until we get 10 relevant records
 * List<Record> records = new ArrayList<>()
 * while (records.size() < 10) {
 *     // ask for a record identifier from a remote service (blocking)
 *     RecordId response = requestIdFromIdServer();
 *     // get the actual record from another service (blocking)
 *     Record record = getRecordFromRecordServer(recordIdentifier);
 *     // only add relevant records
 *     if (isRelevant(record)) {
 *        records.add(record);
 *     }
 * }
 * }</pre>
 * <p>
 * If we wanted to do it without doing any blocking, we can use a pipeline and return a {@link
 * CompletionStage} of the desired record list. Like the blocking version only one request will be
 * made at a time.
 *
 * <pre>{@code
 * CompletionStage<RecordId> requestIdFromIdServer();
 * CompletionStage<Record> getRecordFromRecordServer(RecordId recordId);
 *
 * CompletionStage<List<Response>> responses =
 *   AsyncIterator.generate(this::requestIdFromIdServer) // source iterator
 *  .thenCompose(this::getRecordFromRecordServer)        // intermediate transformation
 *  .filter(record -> isRelevant(record))                // intermediate transformation
 *  .take(10)                                            // intermediate transformation
 *  .collect(Collectors.toList());                       // terminal operation
 *
 * }</pre>
 *
 * <p><b>Intermediate methods</b> - All methods which return {@link AsyncIterator AsyncIterators}
 * are intermediate methods. They can further be broken down into lazy and partially eager methods.
 * Methods that end with the suffix <i>ahead</i> are partially eager, the rest are lazy. A lazy
 * intermediate transformation will not be evaluated until some downstream eager operation is
 * called. Furthermore, only what is needed to satisfy the eager operation will be evaluated from
 * the previous iterator in the chain. When only requesting a single element from the transformed
 * iterator, only a single element may be evaluated from the previous iterator (ex: {@link
 * #thenApply(Function)}), or potentially many elements (ex: {@link #filter(Predicate)}).
 *
 * <p>Methods ending with the suffix <i> ahead </i>, are partially eager. They can be used when
 * there is an expensive transformation step that should be performed in parallel. They will eagerly
 * consume from their upstream iterator up to a specified amount (still sequentially!) and eagerly
 * apply the transformation step.
 *
 * <p>Intermediate methods will propagate exceptions similarly to {@link CompletionStage}, a
 * dependent AsyncIterator will return exceptional stages if the upstream iterator generated
 * exceptional elements.
 *
 * <p><b>Terminal methods</b> - Terminal methods consume the iterator and return a {@link
 * CompletionStage}. After a terminal operation is called, the iterator is considered consumed and
 * should not be used further. If any of the stages in the chain that comprise {@code this} iterator
 * were exceptional, the {@link CompletionStage} returned by a terminal operation will also be
 * exceptional. The exception will short-circuit the terminal operation. For example, a terminal
 * operation such as {@link #forEach(Consumer)} will not to continue to run on subsequent elements
 * of the iterator and instead immediately complete its returned stage with the error. Unless
 * otherwise noted, this behavior holds for all terminal methods but may not documented explicitly.
 *
 * <p>The exception propagation scheme should be familiar to users of {@link CompletionStage},
 * upstream errors will appear wherever the AsyncIterator is consumed and the result is observed
 * (with {@link CompletableFuture#join()} for instance).
 *
 * <p>Unless otherwise noted, methods on this interface are free to throw {@link
 * NullPointerException} if any of the provided arguments are {@code null}.
 *
 * <p>The behavior of an AsyncIterator if {@link #nextStage()} is called after the end of iteration
 * marker is returned is left to the implementation.
 *
 * <p>This interface extends {@link org.hibernate.reactive.util.async.impl.AsyncCloseable}, if there
 * are resources associated with {@code this} iterator that must be relinquished after iteration is
 * complete, the {@link #close()} method should be implemented. Because the majority of methods do
 * not have a manually managed resource, a default implementation of close which does nothing is
 * provided. Terminal methods on this interface do not call {@link #close()}, it is generally the
 * user's responsibility..
 *
 * @param <T> Type of object being iterated over.
 *
 * @see Stream
 */
public interface AsyncIterator<T> extends AsyncCloseable {

	/**
	 * A marker enum that indicates there are no elements left in the iterator.
	 */
	enum End {
		END;

		private static final Either<End, ?> ITERATION_END = Either.left( End.END );

		private static final CompletionStage<? extends Either<AsyncIterator.End, ?>> END_FUTURE =
				completedFuture( ITERATION_END );

		/**
		 * An {@link org.hibernate.reactive.util.async.impl.Either} instance which contains the {@link
		 * End} enum.
		 *
		 * @return An {@link org.hibernate.reactive.util.async.impl.Either} containing the {@link End}
		 * instance
		 */
		@SuppressWarnings("unchecked")
		public static <T> Either<AsyncIterator.End, T> end() {
			return (Either<AsyncIterator.End, T>) ITERATION_END;
		}

		/**
		 * A {@link CompletionStage} which is already complete, and contains the {@link End#end()}
		 * instance as its value.
		 *
		 * @return A completed stage whose value is an {@link
		 * org.hibernate.reactive.util.async.impl.Either} containing the {@link End} instance
		 */
		@SuppressWarnings("unchecked")
		public static <T> CompletionStage<Either<AsyncIterator.End, T>> endStage() {
			return (CompletionStage<Either<AsyncIterator.End, T>>) END_FUTURE;
		}

		@Override
		public String toString() {
			return "End of iteration";
		}
	}

	/**
	 * Returns a stage that will be completed with the next element of {@code this} iterator when it
	 * becomes available, or {@link End} if there are no more elements.
	 *
	 * <p>This is not a terminal method, it can be safely called multiple times. However, this method
	 * is <b>not thread safe</b>, and should only be called in a single-threaded fashion. Moreover,
	 * sequential calls should not be made until the {@link CompletionStage} returned by the previous
	 * call has completed. That is to say,
	 *
	 * <pre>{@code
	 * // illegal
	 * pool.execute(() -> nextStage())
	 * pool.execute(() -> nextStage())
	 *
	 * // just as illegal
	 * f1 = nextStage();
	 * f2 = nextStage();
	 *
	 * // good
	 * nextStage().thenCompose(t -> nextStage());
	 * }</pre>
	 * <p>
	 * Though this is not a terminal method, if a terminal method has been called it is no longer safe
	 * to call this method. When nextStage returns {@link End}, the iterator has no more elements.
	 * After an iterator emits an {@link End} indicator, the result of subsequent calls to nextStage
	 * is undefined.
	 *
	 * <p>An AsyncIterator may be capable of producing normally completing stages after having
	 * producing exceptionally completed stages. nextStage is unique in that it can safely continue to
	 * be called even after a returned stage completes exceptionally, whereas all terminal operations
	 * short circuit when encountering an exception. If a user wishes to continue iteration after
	 * exception, they must use nextStage directly.
	 *
	 * @return A {@link CompletionStage} of the next element for iteration held in the {@link
	 * org.hibernate.reactive.util.async.impl.Either#right()} position, or an instance of {@link
	 * End} held in the {@link org.hibernate.reactive.util.async.impl.Either#left()} position
	 * indicating the end of iteration.
	 */
	CompletionStage<Either<End, T>> nextStage();

	/**
	 * Relinquishes any resources associated with this iterator.
	 *
	 * <p>This method should be overridden if manual resource management is required, the default
	 * implementation does nothing. This method is <b>not</b> thread safe, and must not be called
	 * concurrently with calls to {@link #nextStage()}. This method is not automatically called by
	 * terminal methods, and must be explicitly called after iteration is complete if the underlying
	 * iterator has resources to release. Similar to the situation with {@link Stream#close()},
	 * because the common case requires no resources the user should only call close if it is possible
	 * that the {@link AsyncIterator} has resources. Special care needs to be taken to call close even
	 * in the case of an exception.
	 *
	 * <pre>{@code
	 * class SocketBackedIterator implements AsyncIterator<byte[]> {
	 *  ...
	 *  {@literal @Override}
	 *  CompletionStage<Void> close() { return socket.close(); }
	 * }
	 * AsyncCloseable.tryComposeWith(new SocketBackedIterator(socket), socketIt -> socketIt
	 *  .thenCompose(this::deserialize)
	 *  .filter(this::isRelevantMessage)
	 *  .forEach(message -> System.out.println(message)));
	 * }</pre>
	 * <p>
	 * Intermediate methods will pass calls to close to their upstream iterators, so it is safe to
	 * call close on an intermediate result of an iterator instead of on it directly. For example,
	 *
	 * <pre>{@code
	 * AsyncIterator<byte[]> original = new SocketBackedIterator(socket);
	 * AsyncIterator<Message> transformed = original.thenCompose(this::deserialize).filter(this::isRelevantMessage);
	 *
	 * transformed.close() // will close on original
	 * }</pre>
	 *
	 * @return a {@link CompletionStage} that completes when all resources associated with this
	 * iterator have been relinquished.
	 */
	@Override
	default CompletionStage<Void> close() {
		return CompletionStages.voidFuture();
	}

	/**
	 * Transforms {@code this} into a new AsyncIterator that iterates over the results of {@code fn}
	 * applied to the outcomes of stages in this iterator when they complete normally. When stages in
	 * {@code this} iterator complete exceptionally the returned iterator will emit an exceptional
	 * stage without applying {@code fn}.
	 *
	 * <pre>
	 * {@code
	 * intIterator // 1,2,3,...
	 *     .thenApply(Integer::toString) //"1","2","3"...
	 * }
	 * </pre>
	 * <p>
	 * This is a lazy <i> intermediate </i> method.
	 *
	 * @param fn A function which produces a U from the given T
	 *
	 * @return A new AsyncIterator which produces stages of fn applied to the result of the stages
	 * from {@code this} iterator
	 */
	default <U> AsyncIterator<U> thenApply(final Function<? super T, ? extends U> fn) {
		return AsyncIterators.thenApplyImpl( this, fn, true, null );
	}

	/**
	 * Transforms {@code this} into a new AsyncIterator using the produced stages of {@code fn}
	 * applied to the output from the stages of {@code this}. When stages in {@code this} iterator
	 * complete exceptionally the returned iterator will emit an exceptional stage without applying
	 * {@code fn}.
	 *
	 * <pre>
	 * {@code
	 * CompletableFuture<String> asyncToString(final int i);
	 * intIterator // 1, 2, 3
	 *   .thenCompose(this::asyncToString); //"1", "2", "3"...
	 * }
	 * </pre>
	 * <p>
	 * This is a lazy <i> intermediate </i> method.
	 *
	 * @param fn A function which produces a new {@link CompletionStage} from a T
	 *
	 * @return A new AsyncIterator which produces stages of fn composed with the result of the stages
	 * from {@code this} iterator
	 */
	default <U> AsyncIterator<U> thenCompose(
			final Function<? super T, ? extends CompletionStage<U>> fn) {
		return AsyncIterators.thenComposeImpl( this, fn, true );
	}

	/**
	 * Transforms the AsyncIterator into one which will only produce results that match {@code
	 * predicate}.
	 *
	 * <p>
	 * This is a lazy <i> intermediate </i> method.
	 *
	 * @param predicate A function that takes a T and returns true if it should be returned by the new
	 * iterator, and false otherwise
	 *
	 * @return a new AsyncIterator which will only return results that match predicate
	 */
	default AsyncIterator<T> filter(final Predicate<? super T> predicate) {

		// keep looping looking for a value that satisfies predicate as long as the current value
		// doesn't, and we're not out of elements
		final Predicate<Either<End, T>> shouldKeepLooking =
				either -> either.fold( end -> false, predicate.negate()::test );

		return new AsyncIterator<T>() {
			@Override
			public CompletionStage<Either<End, T>> nextStage() {
				return AsyncIterator.this
						.nextStage()
						.thenCompose(
								t -> AsyncTrampoline.asyncWhile(
										shouldKeepLooking,
										c -> AsyncIterator.this.nextStage(),
										t
								) );
			}

			@Override
			public CompletionStage<Void> close() {
				return AsyncIterator.this.close();
			}
		};
	}

	/**
	 * Collects the results of this iterator in batches, returning an iterator of those batched
	 * collections.
	 *
	 * <p>
	 * This may be useful for performing bulk operations on many elements, rather than on one element
	 * at a time.
	 *
	 * <p>
	 * This is a lazy <i> intermediate </i> method.
	 *
	 * @param collector a {@link Collector} used to collect the elements of this iterator into
	 * individual batches. Each batch will be created by invoking the collector's
	 * {@link Collector#supplier()} method
	 * @param shouldAddToBatch a predicate which determines whether a given element encountered during
	 * iteration should be added to the given (current) batch. If this predicate returns true
	 * for the given element and container, the element will be {@link Collector#accumulator()
	 * added} to the container, and the batching operation will continue to draw from the
	 * underlying iterator. If this predicate returns false, the element will not be added and
	 * the current batch will be {@link Collector#finisher() finished} and returned by the
	 * batching iterator. The element which did not meet the predicate will be tested again by
	 * the next batch
	 *
	 * @return an AsyncIterator which invokes several iterations of the underlying iterator with each
	 * advance, collecting these elements into containers provided by the given
	 * {@link Collector}.
	 */
	default <A, R> AsyncIterator<R> batch(
			final Collector<? super T, A, R> collector,
			final BiPredicate<? super A, ? super T> shouldAddToBatch) {
		return new AsyncIterator<R>() {
			/**
			 * This field holds the result of the latest call to the underlying iterator's 'nextStage'; At
			 * the start of the batching iterator's 'nextStage' method, this holds the value which was
			 * rejected by the last 'addToBatch' call (or empty if the iterator terminated, or null if
			 * this is the first call). If non-End, this rejected value should be tested again in the next
			 * batch. If End, iteration should terminate
			 */
			private Either<End, T> lastAdvance = null;

			@Override
			public CompletionStage<Either<End, R>> nextStage() {
				// the first call has no preceding value to start the batch, so draw from iter
				return this.lastAdvance == null
						? AsyncIterator.this
						.nextStage()
						.thenCompose(
								eitherT -> {
									this.lastAdvance = eitherT;
									return collectBatch();
								} )
						: collectBatch();
			}

			@Override
			public CompletionStage<Void> close() {
				return AsyncIterator.this.close();
			}

			private CompletionStage<Either<End, R>> collectBatch() {
				return this.lastAdvance.fold(
						end -> End.endStage(),
						ignoredT -> {
							final A batch = collector.supplier().get();

							return AsyncTrampoline.asyncWhile(
											eitherT -> eitherT.fold( end -> false, t -> shouldAddToBatch.test( batch, t ) ),
											eitherT -> {
												collector
														.accumulator()
														.accept( batch, eitherT.fold(
																end -> {
																	throw new IllegalStateException();
																},
																t -> t
														) );
												return AsyncIterator.this.nextStage();
											},
											this.lastAdvance
									)
									.thenApply(
											eitherT -> {
												this.lastAdvance = eitherT;
												return Either.right( AsyncIterators.finishContainer(
														batch,
														collector
												) );
											} );
						}
				);
			}
		};
	}

	/**
	 * A convenience method provided to invoke {@link #batch(Collector, BiPredicate)} with a predicate
	 * that limits batches to a fixed size.
	 *
	 * <p>
	 * Each batch will be as large as the given {@code batchSize} except possibly the last one, which
	 * may be smaller due to exhausting the underlying iterator.
	 *
	 * <p>
	 * This is a lazy <i> intermediate </i> method.
	 *
	 * @see #batch(Collector, BiPredicate)
	 */
	default <A, R> AsyncIterator<R> batch(
			final Collector<? super T, A, R> collector, final int batchSize) {
		class CountingContainer {
			final A container;
			int size;

			public CountingContainer(final A container, final int size) {
				this.container = container;
				this.size = size;
			}
		}

		class CountingCollector
				implements Collector<T, CountingContainer, R>,
				Supplier<CountingContainer>,
				BiConsumer<CountingContainer, T>,
				BinaryOperator<CountingContainer>,
				BiPredicate<CountingContainer, T> {
			private final Supplier<A> parentSupplier = collector.supplier();
			private final BiConsumer<A, ? super T> parentAccumulator = collector.accumulator();
			private final BinaryOperator<A> parentCombiner = collector.combiner();
			private final Set<Collector.Characteristics> characteristics;

			CountingCollector() {
				final Set<Collector.Characteristics> characteristics =
						EnumSet.copyOf( collector.characteristics() );
				// remove concurrent (if present) because the increments aren't thread safe
				characteristics.remove( Characteristics.CONCURRENT );

				// remove identity (if present) because the finisher is necessary to unbox the container
				characteristics.remove( Characteristics.IDENTITY_FINISH );
				this.characteristics = Collections.unmodifiableSet( characteristics );
			}

			@Override
			public Supplier<CountingContainer> supplier() {
				return this;
			}

			@Override
			public BiConsumer<CountingContainer, T> accumulator() {
				return this;
			}

			@Override
			public BinaryOperator<CountingContainer> combiner() {
				return this;
			}

			@Override
			public Function<CountingContainer, R> finisher() {
				return countingContainer -> AsyncIterators.finishContainer(
						countingContainer.container,
						collector
				);
			}

			@Override
			public Set<Collector.Characteristics> characteristics() {
				return this.characteristics;
			}

			// supplier
			@Override
			public CountingContainer get() {
				return new CountingContainer( this.parentSupplier.get(), 0 );
			}

			// accumulator
			@Override
			public void accept(final CountingContainer countingContainer, final T t) {
				this.parentAccumulator.accept( countingContainer.container, t );
				countingContainer.size++;
			}

			// combiner
			@Override
			public CountingContainer apply(final CountingContainer c1, final CountingContainer c2) {
				final A combined = this.parentCombiner.apply( c1.container, c2.container );
				// many mutable collectors simply addAll to the left container and return it.
				// this is an optimistic check to save a new container creation
				if ( combined == c1.container ) {
					c1.size += c2.size;
					return c1;
				}
				else {
					return new CountingContainer( combined, c1.size + c2.size );
				}
			}

			// shouldAddToBatch
			@Override
			public boolean test(final CountingContainer countingContainer, final T t) {
				return countingContainer.size < batchSize;
			}
		}

		final CountingCollector counter = new CountingCollector();
		return batch( counter, counter );
	}

	/**
	 * Sequentially accumulates the elements of type T in this iterator into a U. This provides an
	 * immutable style terminal reduction operation as opposed to the mutable style supported by
	 * {@link #collect}. For example, to sum the lengths of Strings in an AsyncIterator, {@code
	 * stringIt.fold(0, (acc, s) -> acc + s.length())}.
	 *
	 * <p>
	 * This is a <i>terminal method</i>.
	 *
	 * @param accumulator a function that produces a new accumulation from an existing accumulation
	 * and a new element
	 * @param identity a starting U value
	 *
	 * @return a {@link CompletionStage} containing the resulting U from repeated application of
	 * accumulator
	 */
	default <U> CompletionStage<U> fold(
			final U identity,
			final BiFunction<U, ? super T, U> accumulator) {
		@SuppressWarnings("unchecked") final U[] uarr = (U[]) new Object[] { identity };
		return this.collect( () -> uarr, (u, t) -> uarr[0] = accumulator.apply( uarr[0], t ) )
				.thenApply( arr -> arr[0] );
	}

	/**
	 * Performs a mutable reduction operation using collector and return a CompletionStage of the
	 * result.
	 *
	 * <p>
	 * This is a <i>terminal method</i>.
	 *
	 * @param collector a {@link Collector} which will sequentially collect the contents of this
	 * iterator into an {@code R}
	 * @param <A> The intermediate type of the accumulated object
	 * @param <R> The final type of the accumulated object
	 *
	 * @return a {@link CompletionStage} which will complete with the collected value
	 *
	 * @see Stream#collect(Collector)
	 */
	default <R, A> CompletionStage<R> collect(final Collector<? super T, A, R> collector) {
		final A container = collector.supplier().get();
		final BiConsumer<A, ? super T> acc = collector.accumulator();
		return forEach( t -> acc.accept( container, t ) )
				.thenApply( ig -> AsyncIterators.finishContainer( container, collector ) );
	}

	/**
	 * Performs a mutable reduction operation and return a {@link CompletionStage} of the result. A
	 * mutable reduction is one where the accumulator has mutable state and additional elements are
	 * incorporated by updating that state.
	 *
	 * <p>
	 * This is a <i>terminal method</i>.
	 *
	 * @param supplier a supplier for a stateful accumulator
	 * @param accumulator a function which can incorporate T elements into a stateful accumulation
	 *
	 * @return a {@link CompletionStage} which will complete with the accumulated value
	 *
	 * @see Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	default <R> CompletionStage<R> collect(
			final Supplier<R> supplier, final BiConsumer<R, ? super T> accumulator) {
		final R container = supplier.get();
		return forEach( t -> accumulator.accept( container, t ) ).thenApply( ig -> container );
	}


	/**
	 * Creates an AsyncIterator for a range.
	 *
	 * <p>
	 * Similar to {@code for(i = start; i < end; i++)}.
	 *
	 * <p>
	 * The stages returned by nextStage will be already completed.
	 *
	 * @param start the start point of iteration (inclusive)
	 * @param end the end point of iteration (exclusive)
	 *
	 * @return an AsyncIterator that will return longs from start to end
	 */
	static AsyncIterator<Long> range(final long start, final long end) {
		if ( start >= end ) {
			return AsyncIterator.empty();
		}
		return new AsyncIterator<Long>() {
			long counter = start;

			@Override
			public CompletionStage<Either<End, Long>> nextStage() {
				if ( this.counter < end ) {
					return completedFuture( Either.right( this.counter++ ) );
				}
				else {
					return End.endStage();
				}
			}
		};
	}

	/**
	 * Performs the side effecting action until the end of iteration is reached
	 *
	 * <p>
	 * This is a <i>terminal method</i>.
	 *
	 * @param action a side-effecting action that takes a T
	 *
	 * @return a {@link CompletionStage} that returns when there are no elements left to apply {@code
	 * action} to, or an exception has been encountered.
	 */
	default CompletionStage<Void> forEach(final Consumer<? super T> action) {
		return AsyncTrampoline.asyncWhile(
				() -> nextStage()
						.thenApply(
								eitherT -> {
									eitherT.forEach(
											ig -> {
											},
											action
									);
									return eitherT.isRight();
								} ) );
	}

	/**
	 * Gets the first element that satisfies predicate, or empty if no such element exists
	 *
	 * <p>
	 * This is a <i>terminal method</i>.
	 *
	 * @param predicate the predicate that returns true for the desired element
	 *
	 * @return a {@link CompletionStage} that completes with the first T to satisfy predicate, or
	 * empty if no such T exists
	 */
	default CompletionStage<Optional<T>> find(final Predicate<? super T> predicate) {
		final CompletionStage<Either<End, T>> future = AsyncIterators
				.convertSynchronousException( this.filter( predicate )::nextStage );
		return future.thenApply( (final Either<End, T> e) -> e.right() );
	}

	/**
	 * Creates an empty AsyncIterator.
	 *
	 * @return an AsyncIterator that will immediately produce an {@link End} marker
	 */
	@SuppressWarnings("unchecked")
	static <T> AsyncIterator<T> empty() {
		return (AsyncIterator<T>) AsyncIterators.EMPTY_ITERATOR;
	}

	/**
	 * Creates an AsyncIterator from an {@link Iterator}
	 *
	 * @param iterator an {@link Iterator} of T elements
	 *
	 * @return A new AsyncIterator which will yield the elements of {@code iterator}
	 */
	static <T> AsyncIterator<T> fromIterator(final Iterator<? extends T> iterator) {
		return () -> completedFuture( iterator.hasNext()
											  ? Either.right( iterator.next() )
											  : End.end() );
	}

	/**
	 * Creates an infinite AsyncIterator of type T.
	 *
	 * @param supplier supplies stages for elements to be yielded by the returned iterator
	 *
	 * @return AsyncIterator returning values generated from {@code supplier}
	 */
	static <T> AsyncIterator<T> generate(final Supplier<? extends CompletionStage<T>> supplier) {
		return () -> supplier.get().thenApply( Either::right );
	}
}

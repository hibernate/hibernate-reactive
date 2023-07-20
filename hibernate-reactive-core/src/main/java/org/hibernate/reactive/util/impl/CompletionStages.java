/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hibernate.reactive.engine.impl.InternalStage;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LogCategory;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import static org.hibernate.reactive.util.async.impl.AsyncIterator.fromIterator;
import static org.hibernate.reactive.util.async.impl.AsyncIterator.range;
import static org.hibernate.reactive.util.async.impl.AsyncTrampoline.asyncWhile;

public class  CompletionStages {

	private static final Log LOG = LoggerFactory.make( Log.class, new LogCategory( "org.hibernate.reactive.errors" ) );

	// singleton instances:
	private static final InternalStage<Void> VOID = completedFuture( null );
	private static final InternalStage<Integer> ZERO = completedFuture( 0 );
	private static final InternalStage<Boolean> TRUE = completedFuture( true );
	private static final InternalStage<Boolean> FALSE = completedFuture( false );

	private static <T> boolean alwaysTrue(T o, int index) {
		return true;
	}

	private static boolean alwaysTrue(int index) {
		return true;
	}

	private static boolean alwaysTrue(Object o) {
		return true;
	}

	private static InternalStage<Boolean> alwaysContinue(Object ignored) {
		return TRUE;
	}

	public static InternalStage<Void> voidFuture(Object ignore) {
		return voidFuture();
	}

	public static InternalStage<Void> voidFuture() {
		return VOID;
	}

	public static InternalStage<Integer> zeroFuture() {
		return ZERO;
	}

	public static InternalStage<Integer> zeroFuture(Object ignore) {
		return zeroFuture();
	}

	public static InternalStage<Boolean> trueFuture() {
		return TRUE;
	}

	public static InternalStage<Boolean> falseFuture() {
		return FALSE;
	}

	public static <T> InternalStage<T> nullFuture() {
		//Unsafe cast, but perfectly fine: avoids having to allocate a new instance
		//for each different "type of null".
		return (InternalStage<T>) VOID;
	}

	public static <T> InternalStage<T> completedFuture(T value) {
		return InternalStage.completedFuture( value );
	}

	public static <T> InternalStage<T> failedFuture(Throwable t) {
		return InternalStage.failedStage( t );
	}

	public static <T extends Throwable, Ret> Ret rethrow(Throwable x) throws T {
		throw (T) x;
	}

	public static <T extends Throwable, Ret> Ret returnNullorRethrow(Throwable x) throws T {
		if ( x != null ) {
			throw (T) x;
		}
		return null;
	}

	public static <T extends Throwable, Ret> Ret returnOrRethrow(Throwable x, Ret result) throws T {
		if ( x != null ) {
			throw (T) x;
		}
		return result;
	}

	/**
	 * For CompletionStage#handle when we don't care about errors
	 */
	public static <U> U ignoreErrors(Void unused, Throwable throwable) {
		return null;
	}

	public static void logSqlException(Throwable t, Supplier<String> message, String sql) {
		if ( t != null ) {
			LOG.failedToExecuteStatement( sql, message.get(), t );
		}
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( i );
	 * }
	 * }</pre>
	 */
	public static InternalStage<Integer> total(int start, int end, IntFunction<InternalStage<Integer>> consumer) {
		return range( start, end )
				.thenCompose( i -> consumer.apply( i.intValue() ) )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * int total = 0;
	 * while( iterator.hasNext() ) {
	 *   total += consumer.apply( iterator.next() );
	 * }
	 * }</pre>
	 */
	public static <T> InternalStage<Integer> total(Iterator<T> iterator, Function<T,InternalStage<Integer>> consumer) {
		return fromIterator( iterator )
				.thenCompose( consumer )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( array[i] );
	 * }
	 * }</pre>
	 */
	public static <T> InternalStage<Integer> total(T[] array, Function<T, InternalStage<Integer>> consumer) {
		return total( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( array[i] );
	 * }
	 * }</pre>
	 */
	public static <T> InternalStage<Void> loop(T[] array, Function<T, InternalStage<?>> consumer) {
		return loop( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test(i) )  {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * }</pre>
	 */
	public static <T> InternalStage<Void> loop(T[] array, IntPredicate filter, IntFunction<InternalStage<?>> consumer) {
		return loop( 0, array.length, filter, consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * int index = 0
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next(), index++ );
	 * }
	 * }</pre>
	 */
	public static <T> InternalStage<Void> loop(Iterator<T> iterator, IntBiFunction<T, InternalStage<?>> consumer) {
		return loop( iterator, CompletionStages::alwaysTrue, consumer );
	}
	/**
	 * Equivalent to:
	 * <pre>{@code
	 * int index = -1
	 * while( iterator.hasNext() ) {
	 *   index++
	 *   T next = iterator.next();
	 *   if (filter.test( next, index ) {
	 *     consumer.apply( next, index );
	 *   }
	 * }
	 * }</pre>
	 */
	public static <T> InternalStage<Void> loop(Iterator<T> iterator, IntBiPredicate<T> filter, IntBiFunction<T, InternalStage<?>> consumer) {
		if ( iterator.hasNext() ) {
			final IndexedIteratorLoop<T> loop = new IndexedIteratorLoop<>( iterator, filter, consumer );
			return asyncWhile( loop::next );
		}
		return voidFuture();
	}

	public static <U> U nullFuture(Void unused) {
		return null;
	}

	public static <R, T extends Throwable> CompletionStageHandler<R, T> handle(R result, T throwable) {
		return new CompletionStageHandler<>( result, throwable );
	}

	public static class CompletionStageHandler<R, T extends Throwable> {

		private final R result;
		private final T throwable;

		public CompletionStageHandler(R result, T throwable) {
			this.result = result;
			this.throwable = throwable;
		}

		public boolean hasFailed() {
			return throwable != null;
		}

		public T getThrowable() {
			return throwable;
		}

		public R getResult() throws T {
			if ( throwable == null ) {
				return result;
			}
			throw (T) throwable;
		}

		public InternalStage<R> getResultAsCompletionStage() {
			if ( throwable == null ) {
				return completedFuture( result );
			}
			return failedFuture( throwable );
		}

		/**
		 * Same as {@link #getResultAsCompletionStage()}, but allows method reference
		 */
		public InternalStage<R> getResultAsCompletionStage(Void unused) {
			return getResultAsCompletionStage();
		}
	}

	/**
	 * It represents a loop on an iterator that requires
	 * an index of the current element.
	 * <p>
	 * Equivalent to:
	 * <pre>{@code
	 *   int index = -1
	 *   while( iterator.hasNext() ) {
	 *     index++
	 *     T next = iterator.next();
	 *     if (filter.test( next, index ) {
	 *       consumer.apply( next, index );
	 *     }
	 *   }
	 * }</pre>
	 * </p>
	 * <p>
	 * This class keeps track of the state of the loop, allowing us to
	 * use an {@code AsyncTrampoline#asyncWhile} via method reference.
	 * </p>
	 * @see org.hibernate.reactive.util.async.impl.AsyncTrampoline
	 * @param <T> the class of the elements in the iterator
	 */
	private static class IndexedIteratorLoop<T> {
		private final IntBiPredicate<T> filter;
		private final IntBiFunction<T, InternalStage<?>> consumer;
		private final Iterator<T> iterator;
		private int currentIndex = -1;
		private T currentEntry;

		public IndexedIteratorLoop(Iterator<T> iterator, IntBiPredicate<T> filter, IntBiFunction<T, InternalStage<?>> consumer) {
			this.iterator = iterator;
			this.filter = filter;
			this.consumer = consumer;
		}

		public InternalStage<Boolean> next() {
			if ( hasNext() ) {
				final T entry = currentEntry;
				final int index = currentIndex;
				return consumer.apply( entry, index )
						.thenCompose( CompletionStages::alwaysContinue );
			}
			return FALSE;
		}

		// Skip all the indexes not matching the filter
		private boolean hasNext() {
			int index = currentIndex;
			T next = currentEntry;
			boolean hasNext = false;
			while ( iterator.hasNext() ) {
				next = iterator.next();
				index++;
				if ( filter.test( next, index ) ) {
					hasNext = true;
					break;
				}
			}
			this.currentEntry = next;
			this.currentIndex = index;
			return hasNext;
		}
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( Object next : iterable ) {
	 *   consumer.apply( next );
	 * }
	 * </pre>
	 */
	public static <T> InternalStage<Void> loop(Collection<T> collection, Function<T, InternalStage<?>> consumer) {
		return loop( collection, CompletionStages::alwaysTrue, consumer );
	}

	/**
	 * @deprecated always prefer the variants which use a List rather than a Collection
	 */
	@Deprecated
	public static <T> InternalStage<Void> loop(Collection<T> collection, Predicate<T> filter, Function<T, InternalStage<?>> consumer) {
		if ( collection instanceof List ) {
			//This is true in almost all known cases, so a good optimisation in practice.
			return loop( (List<T>) collection, filter, consumer );
		}
		else {
			final List<T> list = new ArrayList<>( collection );
			return loop( list, filter, consumer );
		}
	}

	public static <T> InternalStage<Void> loop(List<T> list, Function<T, InternalStage<?>> consumer) {
		return loop( list, CompletionStages::alwaysTrue, consumer );
	}

	public static <T> InternalStage<Void> loop(List<T> list, Predicate<T> filter, Function<T,InternalStage<?>> consumer) {
		return loop( 0, list.size(), index -> filter.test( list.get( index ) ), index -> consumer.apply( list.get( index ) ) );
	}

	public static <T> InternalStage<Void> loop(Queue<T> queue, Function<T, InternalStage<?>> consumer) {
		return loop( queue.iterator(), CompletionStages::alwaysTrue , (value, integer) -> consumer.apply( value ) );
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( i );
	 * }
	 * }</pre>
	 */
	public static InternalStage<Void> loop(int start, int end, IntFunction<InternalStage<?>> consumer) {
		return loop( start, end, CompletionStages::alwaysTrue, consumer );
	}

	public static <T> InternalStage<Void> whileLoop(T[] array, Function<T, InternalStage<Boolean>> consumer) {
		return loop( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	public static InternalStage<Void> whileLoop(int start, int end, IntPredicate filter, IntFunction<InternalStage<Boolean>> consumer) {
		if ( start < end ) {
			final ArrayLoop loop = new ArrayLoop( start, end, filter, consumer);
			return asyncWhile( loop::next );
		}
		return voidFuture();
	}

	/**
	 * Equivalent to:
	 * <pre>{@code
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test(i) ) {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * }</pre>
	 */
	public static InternalStage<Void> loop(int start, int end, IntPredicate filter, IntFunction<InternalStage<?>> consumer) {
		if ( start < end ) {
			final ArrayLoop loop = new ArrayLoop( start, end, filter, index -> consumer
					.apply( index ).thenCompose( CompletionStages::alwaysContinue ) );
			return asyncWhile( loop::next );
		}
		return voidFuture();
	}

	public static InternalStage<Void> whileLoop(Supplier<InternalStage<Boolean>> loopSupplier) {
		return asyncWhile( loopSupplier::get );
	}

	public static InternalStage<Void> whileLoop(Supplier<Boolean> whileCondition, Supplier<InternalStage<?>> loopSupplier) {
		if ( whileCondition.get() ) {
			final WhileLoop whileLoop = new WhileLoop( whileCondition, loopSupplier );
			return asyncWhile( whileLoop::next );
		}
		return voidFuture();
	}

	private static class WhileLoop {

		private final Supplier<InternalStage<?>> loopSupplier;

		private final Supplier<Boolean> whileCondition;

		public WhileLoop(Supplier<Boolean> whileCondition, Supplier<InternalStage<?>> loopSupplier ) {
			this.loopSupplier = loopSupplier;
			this.whileCondition = whileCondition;
		}

		public InternalStage<Boolean> next() {
			if ( whileCondition.get() ) {
				return loopSupplier.get().thenCompose( ignore -> TRUE );
			}
			return FALSE;
		}
	}

	/**
	 * The status of a loop over an array.
	 * <p>
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test( i ) ) && !consumer.apply( i ) {
	 *      break;
	 *   }
	 * }
	 * </pre>
	 * </p>
	 * <p>
	 * This class keeps track of the state of the loop, allowing us to
	 * use an {@code AsyncTrampoline#asyncWhile} via method reference.
	 * </p>
	 */
	private static class ArrayLoop {

		private final IntPredicate filter;

		private final IntFunction<InternalStage<Boolean>> consumer;

		private final int end;
		private int current;

		public ArrayLoop(int start, int end, IntPredicate filter, IntFunction<InternalStage<Boolean>> consumer) {
			this.end = end;
			this.filter = filter;
			this.consumer = consumer;
			this.current = start;
		}

		public InternalStage<Boolean> next() {
			current = next( current );
			if ( current < end ) {
				final int index = current++;
				return consumer.apply( index );
			}
			return FALSE;
		}

		/**
		 * @param start the first index to test
		 * @return the next valid index
		 */
		private int next(int start) {
			int index = start;
			while ( index < end && !filter.test( index ) ) {
				index++;
			}
			return index;
		}
	}

	public static InternalStage<Void> applyToAll(Function<Object, InternalStage<?>> op, Object[] entity) {
		switch ( entity.length ) {
			case 0: return nullFuture();
			case 1: return op.apply( entity[0] ).thenCompose( CompletionStages::voidFuture );
			default: return CompletionStages.loop( entity, op );
		}
	}
}

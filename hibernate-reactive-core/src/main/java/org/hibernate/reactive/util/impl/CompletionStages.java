/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;

import com.ibm.asyncutil.iteration.AsyncIterator;
import com.ibm.asyncutil.iteration.AsyncTrampoline;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class CompletionStages {

	private static final CoreMessageLogger log =
			CoreLogging.messageLogger("org.hibernate.reactive.errors");

	// singleton instances:
	private static final CompletionStage<Void> VOID = completedFuture( null );
	private static final CompletionStage<Integer> ZERO = completedFuture( 0 );
	private static final CompletionStage<Boolean> TRUE = completedFuture( true );
	private static final CompletionStage<Boolean> FALSE = completedFuture( false );

	public static CompletionStage<Void> voidFuture(Object ignore) {
		return voidFuture();
	}

	public static CompletionStage<Void> voidFuture() {
		return VOID;
	}

	public static CompletionStage<Integer> zeroFuture() {
		return ZERO;
	}

	public static CompletionStage<Boolean> trueFuture() {
		return TRUE;
	}
	public static CompletionStage<Boolean> falseFuture() {
		return FALSE;
	}

	public static <T> CompletionStage<T> nullFuture() {
		//Unsafe cast, but perfectly fine: avoids having to allocate a new instance
		//for each different "type of null".
		return (CompletionStage<T>) VOID;
	}

	public static <T> CompletionStage<T> completedFuture(T value) {
		return CompletableFuture.completedFuture( value );
	}

	public static <T> CompletionStage<T> failedFuture(Throwable t) {
		CompletableFuture<T> ret = new CompletableFuture<>();
		ret.completeExceptionally( t );
		return ret;
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

	public static void logSqlException(Throwable t, Supplier<String> message, String sql) {
		if ( t != null ) {
			log.error( "failed to execute statement [" + sql + "]" );
			log.error( message.get(), t );
		}
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( i );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Integer> total(int start, int end, Function<Integer,CompletionStage<Integer>> consumer) {
		return AsyncIterator.range( start, end )
				.thenCompose( i -> consumer.apply( i.intValue() ) )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * while( iterator.hasNext() ) {
	 *   total += consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Integer> total(Iterator<T> iterator, Function<T,CompletionStage<Integer>> consumer) {
		return AsyncIterator.fromIterator( iterator )
				.thenCompose( consumer )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( array[i] );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Integer> total(T[] array, Function<T,CompletionStage<Integer>> consumer) {
		return total( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( array[i] );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(T[] array, Function<T, CompletionStage<?>> consumer) {
		return loop( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	public static <T> CompletionStage<Void> loop(T[] array, Predicate<T> filter, Function<T, CompletionStage<?>> consumer) {
		return loop( 0, array.length, index -> filter.test( array[index] ), index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, BiFunction<T, Integer, CompletionStage<?>> consumer) {
		if ( iterator.hasNext() ) {
			return AsyncTrampoline.asyncWhile(
					index -> iterator.hasNext(),
					index -> consumer
							.apply( iterator.next(), index )
							.thenApply( u -> index + 1 ),
					0
			).thenAccept( CompletionStages::voidFuture );
		}
		return voidFuture();
	}

	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, Function<T, CompletionStage<?>> consumer) {
		if ( iterator.hasNext() ) {
			return AsyncTrampoline.asyncWhile( () -> consumer.apply( iterator.next() )
					.thenApply( r -> iterator.hasNext() ) );
		}
		return voidFuture();
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( Object next : iterable ) {
	 *   consumer.apply( next );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterable<T> iterable, Function<T,CompletionStage<?>> consumer) {
		return loop( iterable.iterator(), consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * Iterator iterator = stream.iterator();
	 * while( iterator.hasNext()) {
	 *   consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Stream<T> stream, Function<T,CompletionStage<?>> consumer) {
		return loop( stream.iterator(), consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( i );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Void> loop(int start, int end, Function<Integer, CompletionStage<?>> consumer) {
		return loop( start, end, index -> true, consumer );
	}

	public static CompletionStage<Void> loop(int start, int end, Predicate<Integer> filter, Function<Integer, CompletionStage<?>> consumer) {
		if ( start < end ) {
			int realStart = next( start, end, filter);
			return AsyncTrampoline.asyncWhile(
					index -> index < end,
					index -> consumer.apply( index )
								.thenApply( u -> next( index + 1, end, filter ) ),
					realStart
			).thenCompose( CompletionStages::voidFuture );
		}
		return voidFuture();
	}

	// Skip all the indexes not matching the filter
	private static int next(int start, int end, Predicate<Integer> filter) {
		int index = start;
		while ( index < end && !filter.test( index ) ) {
			index++;
		}
		return index;
	}

	public static CompletionStage<Void> applyToAll(Function<Object, CompletionStage<?>> op, Object[] entity) {
		switch ( entity.length ) {
			case 0: return nullFuture();
			case 1: return op.apply( entity[0] ).thenCompose( CompletionStages::voidFuture );
			default: return CompletionStages.loop( entity, op );
		}
	}

	/**
	 * Same as {@link #loop(Iterator, Function)} but doesn't use the trampoline pattern
	 */
	public static <T> CompletionStage<Void> loopWithoutTrampoline(Iterator<T> iterator, Function<T, CompletionStage<?>> consumer) {
		CompletionStage<?> loopStage = voidFuture();
		while ( iterator.hasNext() ) {
			final T next = iterator.next();
			loopStage = loopStage.thenCompose( v -> consumer.apply( next ) );
		}
		return loopStage.thenCompose( CompletionStages::voidFuture );
	}
}

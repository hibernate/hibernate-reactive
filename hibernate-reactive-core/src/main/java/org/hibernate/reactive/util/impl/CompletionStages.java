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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CompletionStages {

	private static final CoreMessageLogger log =
			CoreLogging.messageLogger("org.hibernate.reactive.errors");

	public static <T, R> CompletionStage<R> zipArray(
			Function<? super Object[], ? extends R> zipper,
			CompletionStage<? extends T>... sources) {
		Object[] results = new Object[sources.length];
		CompletionStage<?> state = voidFuture();
		for ( int i = 0; i < sources.length; i++ ) {
			CompletionStage<? extends T> completionStage = sources[i];
			int finalI = i;
			CompletionStage<Void> stage = completionStage.thenAccept( result -> {
				results[finalI] = result;
			} );
			state = state.thenCompose( v -> stage );
		}
		return state.thenApply( v -> zipper.apply( results ) );
	}

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
		return completedFuture( null );
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
	public static <T> CompletionStage<Integer> total(int start, int end, Function<Integer,CompletionStage<Integer>> consumer) {
		return AsyncIterator.range( start, end )
				.thenCompose( i -> consumer.apply( i.intValue() ) )
				.fold( 0, (total, next) -> total + next );
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
	public static <T> CompletionStage<Void> loop(T[] array, Function<T,CompletionStage<?>> consumer) {
		return loop( Arrays.stream(array), consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, Function<T,CompletionStage<?>> consumer) {
		if ( iterator.hasNext() ) {
			return AsyncTrampoline.asyncWhile( () -> consumer.apply( iterator.next() )
					.thenApply( r -> iterator.hasNext() ));
		}
		else {
			return voidFuture();
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
	 * Iterator iterator = inStream.iterator();
	 * while( iterator.hasNext()) {
	 *   consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Void> loop(IntStream stream, Function<Integer,CompletionStage<?>> consumer) {
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
	public static CompletionStage<Void> loop(int start, int end, Function<Integer,CompletionStage<?>> consumer) {
		return loop( IntStream.range(start, end), consumer );
	}

	public static CompletionStage<Void> applyToAll(Function<Object, CompletionStage<?>> op, Object[] entity) {
		switch ( entity.length ) {
			case 0: return nullFuture();
			case 1: return op.apply( entity[0] ).thenCompose( CompletionStages::voidFuture );
			default: return CompletionStages.loop( entity, op );
		}
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface InternalStage<T> {

	static <T> InternalStage<T> completedFuture(T value) {
		return new POCInternalStage<>( CompletableFuture.completedFuture( value ) );
	}

	static <T> InternalStage<T> failedStage(Throwable t) {
		CompletableFuture<T> ret = new CompletableFuture<>();
		ret.completeExceptionally( t );
		return new POCInternalStage<>( ret );
	}

	static <T> InternalStage<T> newStage() {
		return new POCInternalStage<>();
	}

	<U> InternalStage<U> thenCompose(Function<? super T, ? extends InternalStage<U>> fn);

	InternalStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

	<U> InternalStage<U> thenApply(Function<? super T,? extends U> fn);

	boolean completeExceptionally(Throwable ex);

	boolean complete(T value);

	<U> InternalStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn);

	InternalStage<Void> thenRun(Runnable action);

	InternalStage<Void> thenAccept(Consumer<? super T> action);

	boolean isDone();

	T getNow(T valueIfAbsent);

	InternalStage<T> exceptionally(Function<Throwable, ? extends T> fn);

	@Deprecated //exists only to simplify migration
	InternalStage<T> toCompletableFuture();

	T join();
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class POCInternalStage<T> implements InternalStage<T> {

	private final CompletableFuture<T> delegate;

	protected POCInternalStage() {
		this( new CompletableFuture<>() );
	}

	POCInternalStage(CompletableFuture<T> delegation) {
		Objects.requireNonNull( delegation );
		this.delegate = delegation;
	}

	@Override
	public <U> InternalStage<U> thenCompose(Function<? super T, ? extends InternalStage<U>> fn) {
		return new POCInternalStage<U>( delegate.thenCompose( convertFunction( fn ) ) );
	}

	private static <U,T> Function<? super T, ? extends CompletionStage<U>> convertFunction( Function<? super T, ? extends InternalStage<U>> fn ) {
		return t -> ( (POCInternalStage) fn.apply( t ) ).delegate;
	}

	@Override
	public InternalStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
		return new POCInternalStage( delegate.whenComplete( action ) );
	}

	@Override
	public <U> InternalStage<U> thenApply(Function<? super T, ? extends U> fn) {
		return new POCInternalStage( delegate.thenApply( fn ) );
	}

	@Override
	public boolean completeExceptionally(Throwable ex) {
		return delegate.completeExceptionally( ex );
	}

	@Override
	public boolean complete(T value) {
		return delegate.complete( value );
	}

	@Override
	public <U> InternalStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
		return new POCInternalStage<>( delegate.handle( fn ) );
	}

	@Override
	public InternalStage<Void> thenRun(Runnable action) {
		return new POCInternalStage<>( delegate.thenRun( action ) );
	}

	@Override
	public InternalStage<Void> thenAccept(Consumer<? super T> action) {
		return new POCInternalStage<>( delegate.thenAccept( action ) );
	}

	@Override
	public boolean isDone() {
		return delegate.isDone();
	}

	@Override
	public T getNow(T valueIfAbsent) {
		return delegate.getNow( valueIfAbsent );
	}

	@Override
	public InternalStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
		return new POCInternalStage<>( delegate.exceptionally( fn ) );
	}

	@Override
	public InternalStage<T> toCompletableFuture() {
		return this;
	}

	@Override
	public T join() {
		return delegate.join();
	}

}

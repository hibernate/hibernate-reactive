/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.Incubating;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.reactive.engine.impl.InternalStage;

public interface ReactiveEventListenerGroup<T> {

	/**
	 * Similar to {@link #fireEventOnEachListener(Object, Function)}, but Reactive friendly: it chains
	 * processing of the same event on each Reactive Listener, and returns a {@link CompletionStage} of type R.
	 * The various generic types allow using this for each concrete event type and flexible return types.
	 * <p>Used by Hibernate Reactive</p>
	 * @param event The event being fired
	 * @param fun The function combining each event listener with the event
	 * @param <R> the return type of the returned CompletionStage
	 * @param <U> the type of the event being fired on each listener
	 * @param <RL> the type of ReactiveListener: each listener of type T will be casted to it.
	 * @return the composite completion stage of invoking fun(event) on each listener.
	 */
	@Incubating
	<R, U, RL> InternalStage<R> fireEventOnEachListener(final U event, final Function<RL, Function<U, InternalStage<R>>> fun);

	/**
	 * Similar to {@link #fireEventOnEachListener(Object, Object, Function)}, but Reactive friendly: it chains
	 * processing of the same event on each Reactive Listener, and returns a {@link CompletionStage} of type R.
	 * The various generic types allow using this for each concrete event type and flexible return types.
	 * <p>Used by Hibernate Reactive</p>
	 * @param event The event being fired
	 * @param fun The function combining each event listener with the event
	 * @param <R> the return type of the returned CompletionStage
	 * @param <U> the type of the event being fired on each listener
	 * @param <RL> the type of ReactiveListener: each listener of type T will be casted to it.
	 * @param <X> an additional parameter to be passed to the function fun
	 * @return the composite completion stage of invoking fun(event) on each listener.
	 */
	@Incubating
	<R, U, RL, X> InternalStage<R> fireEventOnEachListener(U event, X param, Function<RL, BiFunction<U, X, InternalStage<R>>> fun);

	/**
	 * Similar to {@link EventListenerGroup#fireLazyEventOnEachListener(Supplier, BiConsumer)}, but Reactive friendly: it chains
	 * processing of the same event on each Reactive Listener, and returns a {@link CompletionStage} of type R.
	 * The various generic types allow using this for each concrete event type and flexible return types.
	 * <p>This variant expects a Supplier of the event, rather than the event directly; this is useful for the
	 * event types which are commonly configured with no listeners at all, so to allow skipping creating the
	 * event; use only for event types which are known to be expensive while the listeners are commonly empty.</p>
	 * <p>Used by Hibernate Reactive</p>
	 * @param eventSupplier A supplier able to produce the actual event
	 * @param fun The function combining each event listener with the event
	 * @param <R> the return type of the returned CompletionStage
	 * @param <U> the type of the event being fired on each listener
	 * @param <RL> the type of ReactiveListener: each listener of type T will be casted to it.
	 * @return the composite completion stage of invoking fun(event) on each listener.
	 */
	@Incubating
	<R, U, RL> InternalStage<R> fireLazyEventOnEachListener(final Supplier<U> eventSupplier, final Function<RL, Function<U, InternalStage<R>>> fun);

}

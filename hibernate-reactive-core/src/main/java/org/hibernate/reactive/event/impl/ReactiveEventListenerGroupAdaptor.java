/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.reactive.engine.impl.InternalStage;
import org.hibernate.reactive.event.ReactiveEventListenerGroup;

public final class ReactiveEventListenerGroupAdaptor<T> implements ReactiveEventListenerGroup<T> {

	private static final InternalStage COMPLETED = InternalStage.completedFuture( null );
	private final T[] listeners;

	public ReactiveEventListenerGroupAdaptor(EventListenerGroup<T> eventListenerGroup) {
		int size = eventListenerGroup.count();
		final Class type = eventListenerGroup.getEventType().baseListenerInterface();
		this.listeners = (T[]) Array.newInstance( type, size );
		final Iterator<T> listenersIterator = eventListenerGroup.listeners().iterator();//TBD: don't use the deprecated method
		for ( int i = 0; i < size; i++ ) {
			this.listeners[i] = listenersIterator.next();
		}
	}

	@Override
	public <R, U, RL> InternalStage<R> fireEventOnEachListener(
			final U event,
			final Function<RL, Function<U, InternalStage<R>>> fun) {
		InternalStage<R> ret = COMPLETED;
		final T[] ls = listeners;
		if ( ls != null && ls.length != 0 ) {
			for ( T listener : ls ) {
				//to preserve atomicity of the Session methods
				//call apply() from within the arg of thenCompose()
				ret = ret.thenCompose( v -> fun.apply( (RL) listener ).apply( event ) );
			}
		}
		return ret;
	}

	@Override
	public <R, U, RL, X> InternalStage<R> fireEventOnEachListener(
			U event, X param, Function<RL, BiFunction<U, X, InternalStage<R>>> fun) {
		InternalStage<R> ret = COMPLETED;
		final T[] ls = listeners;
		if ( ls != null && ls.length != 0 ) {
			for ( T listener : ls ) {
				//to preserve atomicity of the Session methods
				//call apply() from within the arg of thenCompose()
				ret = ret.thenCompose( v -> fun.apply( (RL) listener ).apply( event, param ) );
			}
		}
		return ret;
	}

	@Override
	public <R, U, RL> InternalStage<R> fireLazyEventOnEachListener(
			final Supplier<U> eventSupplier,
			final Function<RL, Function<U, InternalStage<R>>> fun) {
		InternalStage<R> ret = COMPLETED;
		final T[] ls = listeners;
		if ( ls != null && ls.length != 0 ) {
			final U event = eventSupplier.get();
			for ( T listener : ls ) {
				//to preserve atomicity of the Session methods
				//call apply() from within the arg of thenCompose()
				ret = ret.thenCompose( v -> fun.apply( (RL) listener ).apply( event ) );
			}
		}
		return ret;
	}
}

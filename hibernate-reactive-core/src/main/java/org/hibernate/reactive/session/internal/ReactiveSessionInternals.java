/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.internal;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.session.ReactiveQueryProducer;
import org.hibernate.reactive.session.ReactiveSession;

/**
 * Internal helpers for invoking session-scoped operations without going through
 * the public API.
 * <p>
 * {@link #enqueue} queues an operation behind any previously-started async
 * work on the same session. The other methods bypass that queue, and must be
 * used by operations that are already queued (otherwise they would wait for
 * themselves to complete).
 * <p>
 * Sessions that are not {@link ReactiveSessionImpl} or {@link ReactiveStatelessSessionImpl}
 * don't have a queue, so we can safely delegate to their public methods.
 */
public final class ReactiveSessionInternals {

	private ReactiveSessionInternals() {
	}

	public static <T> CompletionStage<T> enqueue(
			SharedSessionContractImplementor session,
			Supplier<CompletionStage<T>> operation) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			return impl.enqueue( operation );
		}
		if ( session instanceof ReactiveStatelessSessionImpl impl ) {
			return impl.enqueue( operation );
		}
		return operation.get();
	}

	/**
	 * Flush without going through the serialization queue — for use inside
	 * already-enqueued operations (e.g. native query execution).
	 */
	public static CompletionStage<Void> internalReactiveFlush(
			SharedSessionContractImplementor session) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			return impl.doFlush();
		}
		// Sessions without an operation queue can't deadlock
		return ( (ReactiveSession) session ).reactiveFlush();
	}

	public static <T> CompletionStage<T> internalReactiveGet(
			SharedSessionContractImplementor session,
			Class<T> entityClass,
			Object id) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			return impl.internalReactiveGet( entityClass, id );
		}
		return ( (ReactiveSession) session ).reactiveGet( entityClass, id );
	}

	public static <T> CompletionStage<T> internalReactiveFetch(
			SharedSessionContractImplementor session,
			T association,
			boolean unproxy) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			return impl.internalReactiveFetch( association, unproxy );
		}
		if ( session instanceof ReactiveStatelessSessionImpl impl ) {
			return impl.internalReactiveFetch( association, unproxy );
		}
		return ( (ReactiveQueryProducer) session ).reactiveFetch( association, unproxy );
	}
}

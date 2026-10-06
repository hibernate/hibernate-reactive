/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.internal;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.session.ReactiveQueryProducer;

/**
 * Internal helpers for invoking session-scoped operations without going through
 * the public API.
 * <p>
 * {@link #serialized} queues an operation behind any previously-started async
 * work on the same session.  {@link #internalReactiveFetch} fetches an
 * association bypassing that queue, for use inside already-queued operations.
 */
public final class ReactiveSessionInternals {

	private ReactiveSessionInternals() {
	}

	public static <T> CompletionStage<T> serialized(
			SharedSessionContractImplementor session,
			Supplier<CompletionStage<T>> operation) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			return impl.serialized( operation );
		}
		if ( session instanceof ReactiveStatelessSessionImpl impl ) {
			return impl.serialized( operation );
		}
		return operation.get();
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

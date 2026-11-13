/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import java.util.Collection;

import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.exec.internal.lock.LoadedValuesCollectorFactory;
import org.hibernate.sql.exec.internal.lock.LoadedValuesCollectorImpl;

/**
 * A {@link LoadedValuesCollectorImpl} that defers the pessimistic force increments
 * with a {@link ReactiveDirectLockingCompletion}.
 * <p>
 * It also creates the {@link org.hibernate.sql.exec.internal.lock.DirectLockingCompletion}
 * of ORM, so that the entity initializers of ORM see the deferred increments too.
 * </p>
 *
 * @see LoadedValuesCollectorFactory#withDeferredForceIncrement()
 */
public class ReactiveLoadedValuesCollector extends LoadedValuesCollectorImpl {
	private final ReactiveDirectLockingCompletion reactiveDirectLockingCompletion = new ReactiveDirectLockingCompletion();

	public ReactiveLoadedValuesCollector(Collection<NavigablePath> pathsToLock) {
		super( pathsToLock, true );
	}

	public ReactiveDirectLockingCompletion getReactiveDirectLockingCompletion() {
		return reactiveDirectLockingCompletion;
	}

	/**
	 * Reactive version of {@link LoadedValuesCollectorFactory#withDeferredForceIncrement()}
	 */
	public static class Factory extends LoadedValuesCollectorFactory {
		private final Collection<NavigablePath> pathsToLock;

		public Factory(Collection<NavigablePath> pathsToLock) {
			super( pathsToLock );
			this.pathsToLock = pathsToLock;
		}

		@Override
		public LoadedValuesCollectorImpl build() {
			return new ReactiveLoadedValuesCollector( pathsToLock );
		}
	}
}

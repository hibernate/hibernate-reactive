/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;


import java.util.ArrayList;
import java.util.Map;

import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer;

/**
 * @see org.hibernate.sql.results.internal.InitializersList
 */
public final class ReactiveInitializersList {

	private final Initializer[] initializers;
	private final Initializer[] sortedForResolveInstance;
	private final boolean hasCollectionInitializers;

	private ReactiveInitializersList(
			Initializer[] initializers,
			Initializer[] sortedForResolveInstance,
			boolean hasCollectionInitializers) {
		this.initializers = initializers;
		this.sortedForResolveInstance = sortedForResolveInstance;
		this.hasCollectionInitializers = hasCollectionInitializers;
	}

	public boolean hasCollectionInitializers() {
		return this.hasCollectionInitializers;
	}

	static class Builder {
		private final ArrayList<Initializer<?>> initializers;
		int nonCollectionInitializersNum = 0;
		int resolveFirstNum = 0;

		public Builder() {
			initializers = new ArrayList<>();
		}

		public Builder(int size) {
			initializers = new ArrayList<>( size );
		}

		public void addInitializer(final Initializer<?> initializer) {
			initializers.add( initializer );
			//in this method we perform these checks merely to learn the sizing hints,
			//so to not need dynamically scaling collections.
			//This implies performing both checks twice but since they're cheap it's preferable
			//to multiple allocations; not least this allows using arrays, which makes iteration
			//cheaper during the row processing - which is very hot.
			if ( !initializer.isCollectionInitializer() ) {
				nonCollectionInitializersNum++;
			}
			if ( initializeFirst( initializer ) ) {
				resolveFirstNum++;
			}
		}

		private static boolean initializeFirst(final Initializer<?> initializer) {
			return !( initializer instanceof EntityDelayedFetchInitializer ) && !( initializer instanceof EntitySelectFetchInitializer );
		}

		ReactiveInitializersList build(final Map<NavigablePath, Initializer<?>> initializerMap) {
			final int size = initializers.size();
			final Initializer<?>[] sortedForResolveInstance = new Initializer<?>[size];
			int resolveFirstIdx = 0;
			int resolveLaterIdx = resolveFirstNum;
			final Initializer<?>[] originalSortInitializers = toArray( initializers );
			for ( Initializer<?> initializer : originalSortInitializers ) {
				if ( initializeFirst( initializer ) ) {
					sortedForResolveInstance[resolveFirstIdx++] = initializer;
				}
				else {
					sortedForResolveInstance[resolveLaterIdx++] = initializer;
				}
			}
			final boolean hasCollectionInitializers = ( nonCollectionInitializersNum != initializers.size() );
			return new ReactiveInitializersList( originalSortInitializers, sortedForResolveInstance, hasCollectionInitializers );
		}

		private Initializer<?>[] toArray(final ArrayList<Initializer<?>> initializers) {
			return initializers.toArray( new Initializer<?>[initializers.size()] );
		}
	}
}

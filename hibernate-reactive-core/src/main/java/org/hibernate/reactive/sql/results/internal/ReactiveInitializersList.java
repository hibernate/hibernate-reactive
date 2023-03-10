/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;


import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.sql.results.internal.InitializersList
 */
public final class ReactiveInitializersList {

	private final Initializer[] initializers;
	private final Initializer[] sortedNonCollectionsFirst;
	private final Initializer[] sortedForResolveInstance;
	private final boolean hasCollectionInitializers;
	private final Map<NavigablePath, Initializer> initializerMap;

	private ReactiveInitializersList(
			Initializer[] initializers,
			Initializer[] sortedNonCollectionsFirst,
			Initializer[] sortedForResolveInstance,
			boolean hasCollectionInitializers,
			Map<NavigablePath, Initializer> initializerMap) {
		this.initializers = initializers;
		this.sortedNonCollectionsFirst = sortedNonCollectionsFirst;
		this.sortedForResolveInstance = sortedForResolveInstance;
		this.hasCollectionInitializers = hasCollectionInitializers;
		this.initializerMap = initializerMap;
	}

	public Initializer resolveInitializer(final NavigablePath path) {
		return initializerMap.get( path );
	}

	public void finishUpRow(final RowProcessingState rowProcessingState) {
		for ( Initializer init : initializers ) {
			init.finishUpRow( rowProcessingState );
		}
	}

	public CompletionStage<Void> initializeInstance(final ReactiveRowProcessingState rowProcessingState) {
		return loop( initializers, initializer -> {
			if ( initializer instanceof ReactiveInitializer ) {
				return ( (ReactiveInitializer) initializer ).reactiveInitializeInstance( rowProcessingState );
			}
			else {
				initializer.initializeInstance( rowProcessingState );
				return voidFuture();
			}
		} );
	}

	public void endLoading(final ExecutionContext executionContext) {
		for ( Initializer initializer : initializers ) {
			initializer.endLoading( executionContext );
		}
	}

	public void resolveKeys(final RowProcessingState rowProcessingState) {
		for ( Initializer init : sortedNonCollectionsFirst ) {
			init.resolveKey( rowProcessingState );
		}
	}

	public CompletionStage<Void> resolveInstances(final ReactiveRowProcessingState rowProcessingState) {
		return loop( sortedNonCollectionsFirst, initializer -> {
			if ( initializer instanceof ReactiveInitializer ) {
				return ( (ReactiveInitializer) initializer ).reactiveResolveInstance( rowProcessingState );
			}
			else {
				initializer.resolveInstance( rowProcessingState );
				return voidFuture();
			}
		} );
	}

	public boolean hasCollectionInitializers() {
		return this.hasCollectionInitializers;
	}

	static class Builder {
		private final ArrayList<Initializer> initializers = new ArrayList<>();
		int nonCollectionInitializersNum = 0;
		int resolveFirstNum = 0;

		public Builder() {}

		public void addInitializer(final Initializer initializer) {
			initializers.add( initializer );
			//in this method we perform these checks merely to learn the sizing hints,
			//so to not need dynamically scaling collections.
			//This implies performing both checks twice but since they're cheap it's preferrable
			//to multiple allocations; not least this allows using arrays, which makes iteration
			//cheaper during the row processing - which is very hot.
			if ( !initializer.isCollectionInitializer() ) {
				nonCollectionInitializersNum++;
			}
			if ( initializeFirst( initializer ) ) {
				resolveFirstNum++;
			}
		}

		private static boolean initializeFirst(final Initializer initializer) {
			return !( initializer instanceof EntityDelayedFetchInitializer ) && !( initializer instanceof EntitySelectFetchInitializer );
		}

		ReactiveInitializersList build(final Map<NavigablePath, Initializer> initializerMap) {
			final int size = initializers.size();
			final Initializer[] sortedNonCollectionsFirst = new Initializer[size];
			final Initializer[] sortedForResolveInstance = new Initializer[size];
			int nonCollectionIdx = 0;
			int collectionIdx = nonCollectionInitializersNum;
			int resolveFirstIdx = 0;
			int resolveLaterIdx = resolveFirstNum;
			final Initializer[] originalSortInitializers = toArray( initializers );
			for ( Initializer initializer : originalSortInitializers ) {
				if ( initializer.isCollectionInitializer() ) {
					sortedNonCollectionsFirst[collectionIdx++] = initializer;
				}
				else {
					sortedNonCollectionsFirst[nonCollectionIdx++] = initializer;
				}
				if ( initializeFirst( initializer ) ) {
					sortedForResolveInstance[resolveFirstIdx++] = initializer;
				}
				else {
					sortedForResolveInstance[resolveLaterIdx++] = initializer;
				}
			}
			final boolean hasCollectionInitializers = ( nonCollectionInitializersNum != initializers.size() );
			return new ReactiveInitializersList(
					originalSortInitializers,
					sortedNonCollectionsFirst,
					sortedForResolveInstance,
					hasCollectionInitializers,
					initializerMap
			);
		}

		private Initializer[] toArray(final ArrayList<Initializer> initializers) {
			return initializers.toArray( new Initializer[0] );
		}

	}
}

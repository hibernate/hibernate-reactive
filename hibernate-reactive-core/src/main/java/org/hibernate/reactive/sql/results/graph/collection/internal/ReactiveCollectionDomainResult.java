/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.collection.internal;


import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.FetchTiming;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchJoinedImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.Fetchable;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.collection.CollectionInitializer;
import org.hibernate.sql.results.graph.collection.internal.CollectionDomainResult;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchJoinedImpl;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public class ReactiveCollectionDomainResult extends CollectionDomainResult {

	private static final Log LOG = make( Log.class, lookup() );

	public ReactiveCollectionDomainResult(
			NavigablePath loadingPath,
			PluralAttributeMapping loadingAttribute,
			String resultVariable,
			TableGroup tableGroup,
			DomainResultCreationState creationState) {
		super( loadingPath, loadingAttribute, resultVariable, tableGroup, creationState );
	}

	@Override
	public Fetch generateFetchableFetch(
			Fetchable fetchable,
			NavigablePath fetchablePath,
			FetchTiming fetchTiming,
			boolean selected,
			String resultVariable,
			DomainResultCreationState creationState) {
		Fetch fetch = super.generateFetchableFetch(
				fetchable,
				fetchablePath,
				fetchTiming,
				selected,
				resultVariable,
				creationState
		);
		if ( fetch instanceof EntityFetchJoinedImpl ) {
			return new ReactiveEntityFetchJoinedImpl( (EntityFetchJoinedImpl) fetch );
		}
		return fetch;
	}

	@Override
	public CollectionInitializer<?> createInitializer(
			InitializerParent<?> parent,
			AssemblerCreationState creationState) {
		CollectionInitializer<?> initializer = super.createInitializer( parent, creationState );
		return new ReactiveCollectionInitializerAdapter<>( initializer );
	}

	public static class ReactiveCollectionInitializerAdapter<T extends InitializerData>
			implements CollectionInitializer<T> {

		private final CollectionInitializer<T> delegate;

		public ReactiveCollectionInitializerAdapter(CollectionInitializer<T> initializer) {
			this.delegate = initializer;
		}

		@Override
		public NavigablePath getNavigablePath() {
			return delegate.getNavigablePath();
		}

		@Override
		public PluralAttributeMapping getInitializedPart() {
			return delegate.getInitializedPart();
		}

		@Override
		public T getData(RowProcessingState rowProcessingState) {
			return delegate.getData( rowProcessingState );
		}

		@Override
		public void startLoading(RowProcessingState rowProcessingState) {
			delegate.startLoading( rowProcessingState );
		}

		@Override
		public void resolveKey(T data) {
			delegate.resolveKey( data );
		}

		@Override
		public void resolveInstance(T data) {
			delegate.resolveInstance( data );
		}

		@Override
		public void initializeInstance(T data) {
			delegate.initializeInstance( data );
		}

		@Override
		public void finishUpRow(T data) {
			delegate.finishUpRow( data );
		}

		@Override
		public boolean isPartOfKey() {
			return delegate.isPartOfKey();
		}

		@Override
		public boolean isResultInitializer() {
			return delegate.isResultInitializer();
		}

		@Override
		public PersistentCollection<?> getCollectionInstance(T data) {
			return delegate.getCollectionInstance( data );
		}
	}
	}

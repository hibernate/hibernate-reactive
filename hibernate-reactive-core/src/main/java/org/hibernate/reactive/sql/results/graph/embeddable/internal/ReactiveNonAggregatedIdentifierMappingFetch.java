/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;

import org.hibernate.engine.FetchTiming;
import org.hibernate.metamodel.mapping.NonAggregatedIdentifierMapping;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.reactive.metamodel.mapping.internal.ReactiveToOneAttributeMapping;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchJoinedImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.Fetchable;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.embeddable.EmbeddableInitializer;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableFetchImpl;
import org.hibernate.sql.results.graph.embeddable.internal.NonAggregatedIdentifierMappingFetch;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchJoinedImpl;

public class ReactiveNonAggregatedIdentifierMappingFetch extends ReactiveEmbeddableFetchImpl {
	public ReactiveNonAggregatedIdentifierMappingFetch(
			NavigablePath navigablePath,
			NonAggregatedIdentifierMapping embeddedPartDescriptor,
			FetchParent fetchParent,
			FetchTiming fetchTiming,
			boolean hasTableGroup,
			DomainResultCreationState creationState) {
		super( navigablePath, embeddedPartDescriptor, fetchParent, fetchTiming, hasTableGroup, creationState );
	}

	public ReactiveNonAggregatedIdentifierMappingFetch(NonAggregatedIdentifierMappingFetch fetch) {
		super( fetch );
	}

	@Override
	public EmbeddableInitializer<?> createInitializer(
			InitializerParent<?> parent,
			AssemblerCreationState creationState) {
		return new ReactiveNonAggregatedIdentifierMappingInitializer( this, parent, creationState, false );
	}

	@Override
	public Fetch generateFetchableFetch(
			Fetchable fetchable,
			NavigablePath fetchablePath,
			FetchTiming fetchTiming,
			boolean selected,
			String resultVariable,
			DomainResultCreationState creationState) {
		// I don't think this is the right approach.
		// Fetchable should already be an instance of ReactiveToOneAttributeMapping
		// (if it's an instance of ToOneAttributeMapping). But I don't think there is a way right now
		// to make it happen without changing ORM.
		Fetchable reactiveFetchable = fetchable instanceof ToOneAttributeMapping && !( fetchable instanceof ReactiveToOneAttributeMapping )
				? new ReactiveToOneAttributeMapping( (ToOneAttributeMapping) fetchable )
				: fetchable;
		Fetch fetch = super.generateFetchableFetch(
				reactiveFetchable,
				fetchablePath,
				fetchTiming,
				selected,
				resultVariable,
				creationState
		);
		if ( fetch instanceof EmbeddableFetchImpl ) {
			return new ReactiveEmbeddableFetchImpl( (EmbeddableFetchImpl) fetch );
		}
		if ( fetch instanceof EntityFetchJoinedImpl ) {
			return new ReactiveEntityFetchJoinedImpl( (EntityFetchJoinedImpl) fetch );
		}
		return fetch;
	}
}

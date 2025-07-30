/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;

import org.hibernate.engine.FetchTiming;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchSelectImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.Fetchable;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.embeddable.EmbeddableInitializer;
import org.hibernate.sql.results.graph.embeddable.EmbeddableValuedFetchable;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableFetchImpl;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchSelectImpl;

public class ReactiveEmbeddableFetchImpl extends EmbeddableFetchImpl {

	public ReactiveEmbeddableFetchImpl(
			NavigablePath navigablePath,
			EmbeddableValuedFetchable embeddedPartDescriptor,
			FetchParent fetchParent,
			FetchTiming fetchTiming,
			boolean hasTableGroup,
			DomainResultCreationState creationState) {
		super( navigablePath, embeddedPartDescriptor, fetchParent, fetchTiming, hasTableGroup, creationState );
	}

	public ReactiveEmbeddableFetchImpl(EmbeddableFetchImpl original) {
		super( original );
	}

	@Override
	public EmbeddableInitializer<?> createInitializer(InitializerParent<?> parent, AssemblerCreationState creationState) {
		return new ReactiveEmbeddableInitializerImpl( this, getDiscriminatorFetch(), getNullIndicatorResult(), parent, creationState, true );
	}

	@Override
	public DomainResultAssembler<?> createAssembler(InitializerParent<?> parent, AssemblerCreationState creationState) {
		Initializer<?> initializer = creationState.resolveInitializer( this, parent, this );
		EmbeddableInitializer<?> embeddableInitializer = initializer.asEmbeddableInitializer();
		return new ReactiveEmbeddableAssembler( embeddableInitializer );
	}

	@Override
	public Fetch findFetch(Fetchable fetchable) {
		Fetch fetch = super.findFetch( fetchable );
		if ( fetch instanceof EntityFetchSelectImpl entityFetchSelect ) {
			return new ReactiveEntityFetchSelectImpl( entityFetchSelect );
		}
		else if ( fetch instanceof EmbeddableFetchImpl embeddableFetch ) {
			return new ReactiveEmbeddableFetchImpl( embeddableFetch );
		}
		return fetch;
	}
}

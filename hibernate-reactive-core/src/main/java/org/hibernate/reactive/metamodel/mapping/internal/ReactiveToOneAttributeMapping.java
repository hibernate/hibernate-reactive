/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import org.hibernate.engine.FetchTiming;
import org.hibernate.metamodel.mapping.AttributeMetadata;
import org.hibernate.metamodel.mapping.ManagedMappingType;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.reactive.sql.results.graph.embeddable.internal.ReactiveEmbeddableForeignKeyResultImpl;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchJoinedImpl;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchSelectImpl;
import org.hibernate.reactive.sql.results.internal.ReactiveEntityDelayedFetchImpl;
import org.hibernate.reactive.sql.results.internal.domain.ReactiveCircularFetchImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroupProducer;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableForeignKeyResultImpl;
import org.hibernate.sql.results.graph.entity.EntityFetch;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchJoinedImpl;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchSelectImpl;
import org.hibernate.sql.results.internal.domain.CircularFetchImpl;

import static java.util.Objects.requireNonNull;

public class ReactiveToOneAttributeMapping extends ToOneAttributeMapping {

	public ReactiveToOneAttributeMapping(ToOneAttributeMapping delegate) {
		super( requireNonNull( delegate ) );
	}

	@Override
	public EntityFetch generateFetch(
			FetchParent fetchParent,
			NavigablePath fetchablePath,
			FetchTiming fetchTiming,
			boolean selected,
			String resultVariable,
			DomainResultCreationState creationState) {
		EntityFetch entityFetch = super.generateFetch(
				fetchParent,
				fetchablePath,
				fetchTiming,
				selected,
				resultVariable,
				creationState
		);
		if ( entityFetch instanceof EntityFetchJoinedImpl ) {
			return new ReactiveEntityFetchJoinedImpl( (EntityFetchJoinedImpl) entityFetch );
		}
		if ( entityFetch instanceof EntityFetchSelectImpl ) {
			return new ReactiveEntityFetchSelectImpl( (EntityFetchSelectImpl) entityFetch );
		}
		return entityFetch;
	}

	@Override
	public AttributeMetadata getAttributeMetadata() {
		return super.getAttributeMetadata();
	}

	@Override
	public Fetch resolveCircularFetch(
			NavigablePath fetchablePath,
			FetchParent fetchParent,
			FetchTiming fetchTiming,
			DomainResultCreationState creationState) {
		Fetch fetch = super.resolveCircularFetch( fetchablePath, fetchParent, fetchTiming, creationState );
		if ( fetch instanceof CircularFetchImpl ) {
			return new ReactiveCircularFetchImpl( (CircularFetchImpl) fetch );
		}
		return fetch;
	}

	@Override
	protected EntityFetch buildEntityDelayedFetch(
			FetchParent fetchParent,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath navigablePath,
			DomainResult<?> keyResult,
			boolean selectByUniqueKey,
			DomainResultCreationState creationState) {
		return new ReactiveEntityDelayedFetchImpl(
				fetchParent,
				fetchedAttribute,
				navigablePath,
				convert( keyResult ),
				selectByUniqueKey,
				creationState
		);
	}

	private static DomainResult<?> convert(DomainResult<?> keyResult) {
		if ( keyResult instanceof EmbeddableForeignKeyResultImpl ) {
			return new ReactiveEmbeddableForeignKeyResultImpl<>( (EmbeddableForeignKeyResultImpl<?>) keyResult );
		}
		return keyResult;
	}

	@Override
	public ReactiveToOneAttributeMapping copy(
			ManagedMappingType declaringType,
			TableGroupProducer declaringTableGroupProducer) {
		return new ReactiveToOneAttributeMapping( super.copy( declaringType, declaringTableGroupProducer ) );
	}

	@Override
	protected Object lazyInitialize(Object domainValue) {
		return domainValue;
	}
}

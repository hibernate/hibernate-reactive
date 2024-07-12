/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchJoinedImpl;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.embeddable.EmbeddableResultGraphNode;
import org.hibernate.sql.results.graph.embeddable.internal.NonAggregatedIdentifierMappingInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchJoinedImpl;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveNonAggregatedIdentifierMappingInitializer extends NonAggregatedIdentifierMappingInitializer
		implements ReactiveInitializer<NonAggregatedIdentifierMappingInitializer.NonAggregatedIdentifierMappingInitializerData> {

	public ReactiveNonAggregatedIdentifierMappingInitializer(
			EmbeddableResultGraphNode resultDescriptor,
			InitializerParent<?> parent,
			AssemblerCreationState creationState,
			boolean isResultInitializer) {
		super(
				resultDescriptor,
				parent,
				creationState,
				isResultInitializer,
				ReactiveNonAggregatedIdentifierMappingInitializer::convertFetch
		);
	}

	private static Fetch convertFetch(Fetch fetch) {
		if ( fetch instanceof EntityFetchJoinedImpl ) {
			return new ReactiveEntityFetchJoinedImpl( (EntityFetchJoinedImpl) fetch );
		}
		return fetch;
	}

	@Override
	public CompletionStage<Void> reactiveResolveKey(NonAggregatedIdentifierMappingInitializerData data) {
		if ( data.getState() != State.UNINITIALIZED ) {
			return voidFuture();
		}
		// We need to possibly wrap the processing state if the embeddable is within an aggregate
		data.setInstance( null );
		data.setState( State.KEY_RESOLVED );
		if ( getInitializers().length == 0 ) {
			// Resolve the component early to know if the key is missing or not
			return reactiveResolveInstance( data );
		}
		else {
			final RowProcessingState rowProcessingState = data.getRowProcessingState();
			final boolean[] dataIsMissing = {false};
			return loop( getInitializers(), initializer -> {
				if ( dataIsMissing[0] ) {
					return voidFuture();
				}
				final InitializerData subData = ( (ReactiveInitializer<?>) initializer )
						.getData( rowProcessingState );
				return ( (ReactiveInitializer<InitializerData>) initializer )
						.reactiveResolveKey( subData )
						.thenAccept( v -> {
							if ( subData.getState() == State.MISSING ) {
								data.setState( State.MISSING );
								dataIsMissing[0] = true;
							}
						} );
			} );
		}
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(NonAggregatedIdentifierMappingInitializerData data) {
		super.resolveInstance( data );
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(NonAggregatedIdentifierMappingInitializerData data) {
		super.initializeInstance( data );
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> forEachReactiveSubInitializer(
			BiFunction<ReactiveInitializer<?>, RowProcessingState, CompletionStage<Void>> consumer,
			InitializerData data) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		return loop( getInitializers(), initializer -> consumer
				.apply( (ReactiveInitializer<?>) initializer, rowProcessingState )
		);
	}
}

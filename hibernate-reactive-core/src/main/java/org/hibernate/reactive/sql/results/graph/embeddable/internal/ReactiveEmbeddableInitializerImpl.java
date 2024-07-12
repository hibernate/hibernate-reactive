/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.basic.BasicFetch;
import org.hibernate.sql.results.graph.embeddable.EmbeddableResultGraphNode;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableInitializerImpl;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveEmbeddableInitializerImpl extends EmbeddableInitializerImpl
		implements ReactiveInitializer<EmbeddableInitializerImpl.EmbeddableInitializerData> {

	private static class ReactiveEmbeddableInitializerData extends EmbeddableInitializerData {

		public ReactiveEmbeddableInitializerData(
				EmbeddableInitializerImpl initializer,
				RowProcessingState rowProcessingState) {
			super( initializer, rowProcessingState );
		}

		public EmbeddableMappingType.ConcreteEmbeddableType getConcreteEmbeddableType() {
			return super.concreteEmbeddableType;
		}
	}

	public ReactiveEmbeddableInitializerImpl(
			EmbeddableResultGraphNode resultDescriptor,
			BasicFetch<?> discriminatorFetch,
			InitializerParent<?> parent,
			AssemblerCreationState creationState,
			boolean isResultInitializer) {
		super( resultDescriptor, discriminatorFetch, parent, creationState, isResultInitializer );
	}

	@Override
	protected InitializerData createInitializerData(RowProcessingState rowProcessingState) {
		return new ReactiveEmbeddableInitializerData( this, rowProcessingState );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(EmbeddableInitializerData data) {
		super.resolveInstance( data );
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(EmbeddableInitializerData data) {
		super.initializeInstance( data );
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> forEachReactiveSubInitializer(
			BiFunction<ReactiveInitializer<?>, RowProcessingState, CompletionStage<Void>> consumer,
			InitializerData data) {
		final ReactiveEmbeddableInitializerData embeddableInitializerData = (ReactiveEmbeddableInitializerData) data;
		final RowProcessingState rowProcessingState = embeddableInitializerData.getRowProcessingState();
		if ( embeddableInitializerData.getConcreteEmbeddableType() == null ) {
			return loop( subInitializers, subInitializer -> loop( subInitializer, initializer -> consumer
					.apply( (ReactiveInitializer<?>) initializer, rowProcessingState )
			) );
		}
		else {
			Initializer<InitializerData>[] initializers = subInitializers[embeddableInitializerData.getSubclassId()];
			return loop( 0, initializers.length, i -> {
				ReactiveInitializer<?> reactiveInitializer = (ReactiveInitializer<?>) initializers[i];
				return consumer.apply( reactiveInitializer, rowProcessingState );
			} );
		}
	}
}

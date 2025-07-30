/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;


import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.mapping.VirtualModelPart;
import org.hibernate.metamodel.spi.EmbeddableInstantiator;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveDomainResultsAssembler;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.basic.BasicFetch;
import org.hibernate.sql.results.graph.embeddable.EmbeddableResultGraphNode;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableInitializerImpl;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;
import static org.hibernate.sql.results.graph.embeddable.EmbeddableLoadingLogger.EMBEDDED_LOAD_LOGGER;
import static org.hibernate.sql.results.graph.entity.internal.BatchEntityInsideEmbeddableSelectFetchInitializer.BATCH_PROPERTY;

public class ReactiveEmbeddableInitializerImpl extends EmbeddableInitializerImpl
		implements ReactiveInitializer<EmbeddableInitializerImpl.EmbeddableInitializerData> {

	private static class ReactiveEmbeddableInitializerData extends EmbeddableInitializerData {

		public ReactiveEmbeddableInitializerData(
				EmbeddableInitializerImpl initializer,
				RowProcessingState rowProcessingState) {
			super( initializer, rowProcessingState );
		}

		public Object[] getRowState(){
			return rowState;
		}

		@Override
		public void setState(State state) {
			super.setState( state );
			if ( State.UNINITIALIZED == state ) {
				// reset instance to null as otherwise EmbeddableInitializerImpl#prepareCompositeInstance
				//  will never create a new instance after the "first row with a non-null instance" gets processed
				setInstance( null );
			}
		}

		public EmbeddableMappingType.ConcreteEmbeddableType getConcreteEmbeddableType() {
			return super.concreteEmbeddableType;
		}
	}

	public ReactiveEmbeddableInitializerImpl(
			EmbeddableResultGraphNode resultDescriptor,
			BasicFetch<?> discriminatorFetch,
			DomainResult<Boolean> nullIndicatorResult,
			InitializerParent<?> parent,
			AssemblerCreationState creationState,
			boolean isResultInitializer) {
		super( resultDescriptor, discriminatorFetch, nullIndicatorResult, parent, creationState, isResultInitializer );
	}

	@Override
	protected InitializerData createInitializerData(RowProcessingState rowProcessingState) {
		return new ReactiveEmbeddableInitializerData( this, rowProcessingState );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(EmbeddableInitializerData data) {
		if ( data.getState() != State.KEY_RESOLVED ) {
			return voidFuture();
		}

		data.setState( State.RESOLVED );
		return extractRowState( (ReactiveEmbeddableInitializerData) data )
				.thenCompose( unused -> prepareCompositeInstance( (ReactiveEmbeddableInitializerData) data ) );
	}

	private CompletionStage<Void> extractRowState(ReactiveEmbeddableInitializerData data) {
		final DomainResultAssembler<?>[] subAssemblers = assemblers[data.getSubclassId()];
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		final Object[] rowState = data.getRowState();
		final boolean[] stateAllNull = {true};
		final int[] index = {0};
		final boolean[] forceExit = { false };
		return whileLoop(
				() -> index[0] < subAssemblers.length && !forceExit[0],
				() -> {
					final int i = index[0]++;
					final DomainResultAssembler<?> assembler = subAssemblers[i];
					if ( assembler instanceof ReactiveDomainResultsAssembler<?> reactiveAssembler ) {
						return reactiveAssembler.reactiveAssemble( (ReactiveRowProcessingState) rowProcessingState )
								.thenAccept( contributorValue -> setContributorValue(
										contributorValue,
										i,
										rowState,
										stateAllNull,
										forceExit
								) );
					}
					else {
						setContributorValue(
								assembler == null ? null : assembler.assemble( rowProcessingState ),
								i,
								rowState,
								stateAllNull,
								forceExit
						);
						return voidFuture();
					}
		})
				.whenComplete(
						(unused, throwable) -> {
							if ( stateAllNull[0] ) {
								data.setState( State.MISSING );
							}
						}
				);
	}

	private void setContributorValue(
			Object contributorValue,
			int index,
			Object[] rowState,
			boolean[] stateAllNull,
			boolean[] forceExit) {
		if ( contributorValue == BATCH_PROPERTY ) {
			rowState[index] = null;
		}
		else {
			rowState[index] = contributorValue;
		}
		if ( contributorValue != null ) {
			stateAllNull[0] = false;
		}
		else if ( isPartOfKey() ) {
			// If this is a foreign key and there is a null part, the whole thing has to be turned into null
			stateAllNull[0] = true;
			forceExit[0] = true;
		}
	}

	private CompletionStage<Void> prepareCompositeInstance(ReactiveEmbeddableInitializerData data) {
		// Virtual model parts use the owning entity as container which the fetch parent access provides.
		// For an identifier or foreign key this is called during the resolveKey phase of the fetch parent,
		// so we can't use the fetch parent access in that case.
		final ReactiveInitializer<ReactiveEmbeddableInitializerData> parent = (ReactiveInitializer<ReactiveEmbeddableInitializerData>) getParent();
		if ( parent != null && getInitializedPart() instanceof VirtualModelPart && !isPartOfKey() && data.getState() != State.MISSING ) {
			final ReactiveEmbeddableInitializerData subData = parent.getData( data.getRowProcessingState() );
			return parent
					.reactiveResolveInstance( subData )
					.thenCompose(
							unused -> {
								data.setInstance( parent.getResolvedInstance( subData ) );
								if ( data.getState() == State.INITIALIZED ) {
									return voidFuture();
								}
								return doCreateCompositeInstance( data )
										.thenAccept( v -> EMBEDDED_LOAD_LOGGER.debugf(
												"Created composite instance [%s]",
												getNavigablePath()
										) );
							} );
		}

		return doCreateCompositeInstance( data )
				.thenAccept( v -> EMBEDDED_LOAD_LOGGER.debugf( "Created composite instance [%s]", getNavigablePath() ) );

	}

	private CompletionStage<Void> doCreateCompositeInstance(ReactiveEmbeddableInitializerData data) {
		if ( data.getInstance() == null ) {
			return createCompositeInstance( data )
					.thenAccept( data::setInstance );
		}
		return voidFuture();
	}

	private CompletionStage<Object> createCompositeInstance(ReactiveEmbeddableInitializerData data) {
		if ( data.getState() == State.MISSING ) {
			return nullFuture();
		}

		final EmbeddableInstantiator instantiator = data.getConcreteEmbeddableType() == null
				? getInitializedPart().getEmbeddableTypeDescriptor().getRepresentationStrategy().getInstantiator()
				: data.getConcreteEmbeddableType().getInstantiator();
		final Object instance = instantiator.instantiate( data );
		data.setState( State.RESOLVED );
		return completedFuture( instance );
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
			return loop( subInitializers, subInitializer ->
					loop( subInitializer, initializer ->
							initializer != null
									? consumer.apply( (ReactiveInitializer<?>) initializer, rowProcessingState )
									: voidFuture()
					)
			);
		}
		else {
			Initializer<InitializerData>[] initializers = subInitializers[embeddableInitializerData.getSubclassId()];
			return loop(0, initializers.length, i ->
					initializers[i] != null
							? consumer.apply( (ReactiveInitializer<?>) initializers[i], rowProcessingState )
							: voidFuture()
			);
		}
	}

	@Override
	public void resolveInstance(EmbeddableInitializerData data) {
		// We need to clean up the instance, otherwise the .find with multiple id is not going to work correctly.
		// It will only return the first element of the list. See EmbeddedIdTest#testFindMultipleIds.
		// ORM doesn't have this issue because they don't have a find with multiple ids.
		data.setInstance( null );
		super.resolveInstance( data );
	}

	@Override
	public Object getResolvedInstance(EmbeddableInitializerData data) {
		return super.getResolvedInstance( data );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.generator.GeneratorCreationContext;
import org.hibernate.id.CompositeNestedGeneratedValueGenerator;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.mapping.Component;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.tuple.entity.ReactiveEntityMetamodel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

public class ReactiveCompositeNestedGeneratedValueGenerator extends CompositeNestedGeneratedValueGenerator implements
		ReactiveIdentifierGenerator<Object> {

	public ReactiveCompositeNestedGeneratedValueGenerator(
			CompositeNestedGeneratedValueGenerator generator,
			GeneratorCreationContext creationContext,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		super(
				generator.getGenerationContextLocator(),
				generator.getCompositeType(),
				reactivePlans( generator, creationContext, runtimeModelCreationContext )
		);
	}

	private static List<GenerationPlan> reactivePlans(
			CompositeNestedGeneratedValueGenerator generator,
			GeneratorCreationContext creationContext,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		final List<GenerationPlan> plans = new ArrayList<>();
		for ( GenerationPlan plan : generator.getGenerationPlans() ) {
			final GenerationPlan reactivePlane = new Component.ValueGenerationPlan(
					(BeforeExecutionGenerator) ReactiveEntityMetamodel.augmentWithReactiveGenerator(
							plan.getGenerator(),
							creationContext,
							runtimeModelCreationContext
					),
					plan.getInjector(),
					plan.getPropertyIndex()
			);
			plans.add( reactivePlane );
		}
		return plans;
	}

	@Override
	public CompletionStage<Object> generate(ReactiveConnectionSupplier reactiveConnectionSupplier, Object object) {
		SharedSessionContractImplementor session = (SharedSessionContractImplementor) reactiveConnectionSupplier;
		final Object context = getGenerationContextLocator().locateGenerationContext( session, object );

		final List<Object> generatedValues = getCompositeType().isMutable()
				? null
				: new ArrayList<>( getGenerationPlans().size() );
		return loop( getGenerationPlans(), generationPlan -> generateIdentifier(
						reactiveConnectionSupplier,
						object,
						generationPlan,
						session,
						generatedValues,
						context
				) )
				.thenCompose( v -> handleGeneratedValues( generatedValues, context, session ) );
	}

	private CompletionStage<?> generateIdentifier(
			ReactiveConnectionSupplier reactiveConnectionSupplier,
			Object object,
			GenerationPlan generationPlan,
			SharedSessionContractImplementor session,
			List<Object> generatedValues,
			Object context) {
		final Generator generator = generationPlan.getGenerator();
		if ( generator.generatedBeforeExecution( object, session ) ) {
			if ( generator instanceof ReactiveIdentifierGenerator<?> reactiveIdentifierGenerator ) {
				return reactiveIdentifierGenerator
						.generate( reactiveConnectionSupplier, object )
						.thenAccept( generated -> {
							if ( generatedValues != null ) {
								generatedValues.add( generated );
							}
							else {
								generationPlan.getInjector().set( context, generated );
							}
						} );
			}
			else {
				final Object currentValue = generator.allowAssignedIdentifiers()
						? getCompositeType().getPropertyValue( context, generationPlan.getPropertyIndex(), session )
						: null;
				return completedFuture( ( (BeforeExecutionGenerator) generator )
												.generate( session, object, currentValue, INSERT ) );
			}
		}
		else {
			throw new IdentifierGenerationException( "Identity generation isn't supported for composite ids" );
		}
	}

	private CompletionStage<Object> handleGeneratedValues(
			List<Object> generatedValues,
			Object context,
			SharedSessionContractImplementor session) {
		if ( generatedValues != null ) {
			final Object[] values = getCompositeType().getPropertyValues( context );
			for ( int i = 0; i < generatedValues.size(); i++ ) {
				values[getGenerationPlans().get( i ).getPropertyIndex()] = generatedValues.get( i );
			}
			return completedFuture( getCompositeType().replacePropertyValues( context, values, session ) );
		}
		else {
			return completedFuture( context );
		}
	}
}

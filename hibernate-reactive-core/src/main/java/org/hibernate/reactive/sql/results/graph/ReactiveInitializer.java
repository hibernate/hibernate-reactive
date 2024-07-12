/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.Incubating;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;


/**
 * @see org.hibernate.sql.results.graph.Initializer
 */
@Incubating
public interface ReactiveInitializer<Data extends InitializerData> {

	/**
	 * The current data of this initializer.
	 */
	Data getData(RowProcessingState rowProcessingState);

	CompletionStage<Void> reactiveResolveInstance(Data data);

	/**
	 * @see org.hibernate.sql.results.graph.internal.AbstractInitializer#resolveKey(InitializerData)
	 */
	default CompletionStage<Void> reactiveResolveKey(Data data) {
		data.setState( Initializer.State.KEY_RESOLVED );
		return forEachReactiveSubInitializer( ReactiveInitializer::reactiveResolveKey, data );
	}

	default CompletionStage<Void> reactiveResolveKey(RowProcessingState rowProcessingState) {
		Data data = getData( rowProcessingState );
		return reactiveResolveKey( data );
	}

	default CompletionStage<Void> reactiveResolveInstance(Object instance, Data data) {
		return reactiveResolveKey( data );
	}

	default CompletionStage<Void> reactiveResolveInstance(Object instance, RowProcessingState rowProcessingState) {
		return reactiveResolveInstance( instance, getData( rowProcessingState ) );
	}

	default CompletionStage<Void> reactiveResolveInstance(ReactiveRowProcessingState rowProcessingState) {
		return reactiveResolveInstance( getData( rowProcessingState ) );
	}

	CompletionStage<Void> reactiveInitializeInstance(Data data);

	default CompletionStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState) {
		return reactiveInitializeInstance( getData( rowProcessingState ) );
	}

	CompletionStage<Void> forEachReactiveSubInitializer(
			BiFunction<ReactiveInitializer<?>, RowProcessingState, CompletionStage<Void>> consumer,
			InitializerData data);

}

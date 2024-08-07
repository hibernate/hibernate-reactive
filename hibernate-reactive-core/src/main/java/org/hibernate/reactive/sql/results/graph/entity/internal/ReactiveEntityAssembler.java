/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveDomainResultsAssembler;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntityAssembler;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.type.descriptor.java.JavaType;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntityAssembler
 */
public class ReactiveEntityAssembler extends EntityAssembler implements ReactiveDomainResultsAssembler {

	public <T> ReactiveEntityAssembler(JavaType<T> javaType, EntityInitializer initializer) {
		super( javaType, initializer );
	}

	@Override
	public CompletionStage<Object> reactiveAssemble(ReactiveRowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		// Ensure that the instance really is initialized
		// This is important for key-many-to-ones that are part of a collection key fk,
		// as the instance is needed for resolveKey before initializing the instance in RowReader
		final ReactiveInitializer<InitializerData> reactiveInitializer = (ReactiveInitializer<InitializerData>) getInitializer();
		final InitializerData data = reactiveInitializer.getData( rowProcessingState );
		final Initializer.State state = data.getState();
		if ( state == Initializer.State.KEY_RESOLVED ) {
			return reactiveInitializer
					.reactiveResolveInstance( data )
					.thenApply( v -> reactiveInitializer.getResolvedInstance( data ) );
		}
		return completedFuture( reactiveInitializer.getResolvedInstance( data ) );
	}
}

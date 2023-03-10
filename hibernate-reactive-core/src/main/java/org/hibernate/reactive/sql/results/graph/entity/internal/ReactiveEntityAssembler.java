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
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.type.descriptor.java.JavaType;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntityAssembler
 */
public class ReactiveEntityAssembler<T> implements ReactiveDomainResultsAssembler<T> {

	private final JavaType<T> javaType;
	private final EntityInitializer initializer;

	public ReactiveEntityAssembler(JavaType<T> javaType, EntityInitializer initializer) {
		this.javaType = javaType;
		this.initializer = initializer;
	}

	@Override
	public JavaType<T> getAssembledJavaType() {
		return javaType;
	}

	@Override
	public CompletionStage<T> reactiveAssemble(ReactiveRowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		// Ensure that the instance really is initialized
		// This is important for key-many-to-ones that are part of a collection key fk,
		// as the instance is needed for resolveKey before initializing the instance in RowReader
		return ( (ReactiveInitializer) initializer )
				.reactiveResolveInstance( rowProcessingState )
				.thenApply( v -> (T) initializer.getEntityInstance() );

	}
}

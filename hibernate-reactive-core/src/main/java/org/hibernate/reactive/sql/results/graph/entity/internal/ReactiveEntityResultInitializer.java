/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;


import org.hibernate.LockMode;
import org.hibernate.reactive.sql.results.graph.entity.ReactiveAbstractEntityInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.basic.BasicFetch;
import org.hibernate.sql.results.graph.entity.EntityResultGraphNode;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntityResultInitializer
 */
public class ReactiveEntityResultInitializer extends ReactiveAbstractEntityInitializer {
	private static final String CONCRETE_NAME = ReactiveEntityResultInitializer.class.getSimpleName();

	public ReactiveEntityResultInitializer(
			EntityResultGraphNode resultDescriptor,
			NavigablePath navigablePath,
			LockMode lockMode,
			Fetch identifierFetch,
			BasicFetch<?> discriminatorFetch,
			DomainResult<Object> rowIdResult,
			AssemblerCreationState creationState) {
		super(
				resultDescriptor,
				navigablePath,
				lockMode,
				identifierFetch,
				discriminatorFetch,
				rowIdResult,
				creationState
		);
	}

	@Override
	protected String getSimpleConcreteImplName() {
		return CONCRETE_NAME;
	}

	@Override
	protected boolean isEntityReturn() {
		return true;
	}

	@Override
	public String toString() {
		return CONCRETE_NAME + "(" + getNavigablePath() + ")";
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal.domain;

import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityDelayedFetchInitializer;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntitySelectFetchInitializerBuilder;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.basic.BasicFetch;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.internal.domain.CircularFetchImpl;

/**
 * @see org.hibernate.sql.results.internal.domain.CircularFetchImpl
 */
public class ReactiveCircularFetchImpl extends CircularFetchImpl {
	public ReactiveCircularFetchImpl(CircularFetchImpl original) {
		super( original );
	}

	@Override
	protected EntityInitializer<?> buildEntitySelectFetchInitializer(
			InitializerParent<?> parent,
			ToOneAttributeMapping fetchable,
			EntityPersister entityPersister,
			DomainResult<?> keyResult,
			NavigablePath navigablePath,
			boolean selectByUniqueKey,
			AssemblerCreationState creationState) {
		return ReactiveEntitySelectFetchInitializerBuilder.createInitializer(
				parent,
				fetchable,
				entityPersister,
				keyResult,
				navigablePath,
				selectByUniqueKey,
				false,
				creationState
		);
	}

	@Override
	protected EntityInitializer<?> buildEntityDelayedFetchInitializer(
			InitializerParent<?> parent,
			NavigablePath referencedPath,
			ToOneAttributeMapping fetchable,
			boolean selectByUniqueKey,
			DomainResult<?> keyResult,
			BasicFetch<?> discriminatorFetch,
			AssemblerCreationState creationState) {
		return new ReactiveEntityDelayedFetchInitializer(
				parent,
				referencedPath,
				fetchable,
				selectByUniqueKey,
				keyResult,
				discriminatorFetch,
				creationState
		);
	}
}

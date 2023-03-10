/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchSelectImpl;

public class ReactiveEntityFetchSelectImpl extends EntityFetchSelectImpl {

	public ReactiveEntityFetchSelectImpl(EntityFetchSelectImpl original) {
		super( original );
	}

	@Override
	protected Initializer buildEntitySelectFetchInitializer(
			FetchParentAccess parentAccess,
			ToOneAttributeMapping fetchedMapping,
			EntityPersister entityPersister,
			DomainResult<?> keyResult,
			NavigablePath navigablePath,
			boolean selectByUniqueKey,
			AssemblerCreationState creationState) {
		return ReactiveEntitySelectFetchInitializerBuilder.createInitializer(
				parentAccess,
				fetchedMapping,
				entityPersister,
				keyResult,
				navigablePath,
				selectByUniqueKey,
				creationState
		);
	}

	@Override
	protected DomainResultAssembler<?> buildEntityAssembler(Initializer initializer) {
		return new ReactiveEntityAssembler<>( getResultJavaType(), initializer.asEntityInitializer() );
	}
}

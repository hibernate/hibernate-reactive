/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.LockMode;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.EntityResultGraphNode;
import org.hibernate.sql.results.graph.entity.EntityValuedFetchable;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchJoinedImpl;

public class ReactiveEntityFetchJoinedImpl extends EntityFetchJoinedImpl {
	public ReactiveEntityFetchJoinedImpl(EntityFetchJoinedImpl entityFetch) {
		super( entityFetch );
	}

	@Override
	protected Initializer buildEntityJoinedFetchInitializer(
			EntityResultGraphNode resultDescriptor,
			EntityValuedFetchable referencedFetchable,
			NavigablePath navigablePath,
			LockMode lockMode,
			NotFoundAction notFoundAction,
			DomainResult<?> keyResult,
			Fetch identifierFetch,
			Fetch discriminatorFetch,
			AssemblerCreationState creationState) {
		return new ReactiveEntityJoinedFetchInitializer(
				resultDescriptor,
				referencedFetchable,
				navigablePath,
				lockMode,
				notFoundAction,
				keyResult,
				identifierFetch,
				discriminatorFetch,
				creationState
		);
	}
}

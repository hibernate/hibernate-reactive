/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchJoinedImpl;

public class ReactiveEntityFetchJoinedImpl extends EntityFetchJoinedImpl {
	public ReactiveEntityFetchJoinedImpl(EntityFetchJoinedImpl entityFetch) {
		super( entityFetch );
	}

	@Override
	public EntityInitializer createInitializer(FetchParentAccess parentAccess, AssemblerCreationState creationState) {
		return new ReactiveEntityJoinedFetchInitializer(
				getEntityResult(),
				getReferencedModePart(),
				getNavigablePath(),
				creationState.determineEffectiveLockMode( getSourceAlias() ),
				getNotFoundAction(),
				getKeyResult(),
				getEntityResult().getRowIdResult(),
				getEntityResult().getIdentifierFetch(),
				getEntityResult().getDiscriminatorFetch(),
				parentAccess,
				creationState
		);
	}
}

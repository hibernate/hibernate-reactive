/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityDelayedFetchInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchImpl;

public class ReactiveEntityDelayedFetchImpl extends EntityDelayedFetchImpl {
	public ReactiveEntityDelayedFetchImpl(
			FetchParent fetchParent,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath navigablePath,
			DomainResult<?> keyResult,
			boolean selectByUniqueKey) {
		super( fetchParent, fetchedAttribute, navigablePath, keyResult, selectByUniqueKey );
	}

	@Override
	public EntityInitializer createInitializer(FetchParentAccess parentAccess, AssemblerCreationState creationState) {
		return new ReactiveEntityDelayedFetchInitializer( parentAccess,
														  getNavigablePath(),
														  getEntityValuedModelPart(),
														  isSelectByUniqueKey(),
														  getKeyResult().createResultAssembler(
																  parentAccess,
																  creationState
														  )
		);
	}
}

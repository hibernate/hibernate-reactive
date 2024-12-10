/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityAssembler;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityDelayedFetchInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntityAssembler;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchImpl;

public class ReactiveEntityDelayedFetchImpl extends EntityDelayedFetchImpl {
	public ReactiveEntityDelayedFetchImpl(
			FetchParent fetchParent,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath navigablePath,
			DomainResult<?> keyResult,
			boolean selectByUniqueKey,
			DomainResultCreationState creationState) {
		super( fetchParent, fetchedAttribute, navigablePath, keyResult, selectByUniqueKey, creationState );
	}

	@Override
	public EntityInitializer<?> createInitializer(InitializerParent<?> parent, AssemblerCreationState creationState) {
		return new ReactiveEntityDelayedFetchInitializer(
				parent,
				getNavigablePath(),
				getEntityValuedModelPart(),
				isSelectByUniqueKey(),
				getKeyResult(),
				getDiscriminatorFetch(),
				creationState
		);
	}

	@Override
	protected EntityAssembler<?> buildEntityAssembler(EntityInitializer<?> entityInitializer) {
		return new ReactiveEntityAssembler( getFetchedMapping().getJavaType(), entityInitializer );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.EntityAssembler;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchSelectImpl;

public class ReactiveEntityFetchSelectImpl extends EntityFetchSelectImpl {

	public ReactiveEntityFetchSelectImpl(EntityFetchSelectImpl original) {
		super( original );
	}

	@Override
	public EntityInitializer<?> createInitializer(InitializerParent<?> parent, AssemblerCreationState creationState) {
		return ReactiveEntitySelectFetchInitializerBuilder.createInitializer(
				parent,
				getFetchedMapping(),
				getReferencedMappingContainer().getEntityPersister(),
				getKeyResult(),
				getNavigablePath(),
				isSelectByUniqueKey(),
				isAffectedByFilter(),
				creationState
		);
	}

	@Override
	protected EntityAssembler buildEntityAssembler(EntityInitializer entityInitializer) {
		return new ReactiveEntityAssembler( getFetchedMapping().getJavaType(), entityInitializer );
	}
}

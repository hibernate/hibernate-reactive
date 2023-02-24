/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategyProvider;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveCteInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveCteMutationStrategy;

public class ReactiveSqmMultiTableMutationStrategyProvider implements SqmMultiTableMutationStrategyProvider {

	@Override
	public SqmMultiTableMutationStrategy createMutationStrategy(
			EntityMappingType rootEntityDescriptor,
			MappingModelCreationProcess creationProcess) {
		final RuntimeModelCreationContext creationContext = creationProcess.getCreationContext();

		//TODO there's more flavours in ORM ? And what do we do about explicitly configured instances & Dialect produced implementations?
		return new ReactiveCteMutationStrategy( rootEntityDescriptor, creationContext );
	}

	@Override
	public SqmMultiTableInsertStrategy createInsertStrategy(
			EntityMappingType rootEntityDescriptor,
			MappingModelCreationProcess creationProcess) {
		final RuntimeModelCreationContext creationContext = creationProcess.getCreationContext();

		//TODO there's more flavours in ORM ? And what do we do about explicitly configured instances & Dialect produced implementations?
		return new ReactiveCteInsertStrategy( rootEntityDescriptor, creationContext );
	}

}

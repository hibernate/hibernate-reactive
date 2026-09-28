/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.mutation.spi.MultiTableMutationStrategyKind;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategyProvider;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveCteInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveCteMutationStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveGlobalTemporaryTableInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveGlobalTemporaryTableMutationStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveLocalTemporaryTableInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveLocalTemporaryTableMutationStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactivePersistentTableInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactivePersistentTableMutationStrategy;

public class ReactiveSqmMultiTableMutationStrategyProvider implements SqmMultiTableMutationStrategyProvider {

	@Override
	public SqmMultiTableMutationStrategy createMutationStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext creationContext) {
		final Dialect dialect = creationContext.getDialect();
		final MultiTableMutationStrategyKind kind =
				dialect.getMultiTableMutationSupport().mutationStrategyKind();
		switch ( kind ) {
			case CTE:
				return new ReactiveCteMutationStrategy( rootEntityDescriptor, creationContext );
			case LOCAL_TEMPORARY_TABLE:
				return new ReactiveLocalTemporaryTableMutationStrategy( rootEntityDescriptor, creationContext );
			case GLOBAL_TEMPORARY_TABLE:
				return new ReactiveGlobalTemporaryTableMutationStrategy( rootEntityDescriptor, creationContext );
			case PERSISTENT_TABLE:
				return new ReactivePersistentTableMutationStrategy( rootEntityDescriptor, creationContext );
			default:
				throw new IllegalArgumentException( "Unrecognized MultiTableMutationStrategyKind: " + kind );
		}
	}

	@Override
	public SqmMultiTableInsertStrategy createInsertStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext creationContext) {
		final Dialect dialect = creationContext.getDialect();
		final MultiTableMutationStrategyKind kind =
				dialect.getMultiTableMutationSupport().insertStrategyKind();
		switch ( kind ) {
			case CTE:
				return new ReactiveCteInsertStrategy( rootEntityDescriptor, creationContext );
			case LOCAL_TEMPORARY_TABLE:
				return new ReactiveLocalTemporaryTableInsertStrategy( rootEntityDescriptor, creationContext );
			case GLOBAL_TEMPORARY_TABLE:
				return new ReactiveGlobalTemporaryTableInsertStrategy( rootEntityDescriptor, creationContext );
			case PERSISTENT_TABLE:
				return new ReactivePersistentTableInsertStrategy( rootEntityDescriptor, creationContext );
			default:
				throw new IllegalArgumentException( "Unrecognized MultiTableMutationStrategyKind: " + kind );
		}
	}
}

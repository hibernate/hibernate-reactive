/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.mutation.internal.cte.CteInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.cte.CteMutationStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableMutationStrategy;
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
			MappingModelCreationProcess creationProcess) {
		final RuntimeModelCreationContext creationContext = creationProcess.getCreationContext();
		SqmMultiTableMutationStrategy mutationStrategy = mutationStrategy( rootEntityDescriptor, creationContext );
		if ( mutationStrategy instanceof CteMutationStrategy ) {
			return new ReactiveCteMutationStrategy( rootEntityDescriptor, creationContext );
		}
		if ( mutationStrategy instanceof LocalTemporaryTableMutationStrategy ) {
			return new ReactiveLocalTemporaryTableMutationStrategy( (LocalTemporaryTableMutationStrategy) mutationStrategy );
		}
		if ( mutationStrategy instanceof PersistentTableMutationStrategy ) {
			return new ReactivePersistentTableMutationStrategy( (PersistentTableMutationStrategy) mutationStrategy );
		}
		if ( mutationStrategy instanceof GlobalTemporaryTableMutationStrategy ) {
			return new ReactiveGlobalTemporaryTableMutationStrategy( (GlobalTemporaryTableStrategy) mutationStrategy );
		}
		return mutationStrategy;
	}

	private static SqmMultiTableMutationStrategy mutationStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext creationContext) {
		final SessionFactoryOptions options = creationContext.getSessionFactoryOptions();
		return options.getCustomSqmMultiTableMutationStrategy() != null
				? options.getCustomSqmMultiTableMutationStrategy()
				: creationContext.getDialect().getFallbackSqmMutationStrategy( rootEntityDescriptor, creationContext );
	}

	@Override
	public SqmMultiTableInsertStrategy createInsertStrategy(
			EntityMappingType rootEntityDescriptor,
			MappingModelCreationProcess creationProcess) {
		final RuntimeModelCreationContext creationContext = creationProcess.getCreationContext();
		final SqmMultiTableInsertStrategy insertStrategy = insertStrategy( rootEntityDescriptor, creationContext );
		if ( insertStrategy instanceof CteInsertStrategy ) {
			return new ReactiveCteInsertStrategy( rootEntityDescriptor, creationContext );
		}
		if ( insertStrategy instanceof LocalTemporaryTableInsertStrategy ) {
			return new ReactiveLocalTemporaryTableInsertStrategy( (LocalTemporaryTableInsertStrategy) insertStrategy );
		}
		if ( insertStrategy instanceof PersistentTableInsertStrategy ) {
			return new ReactivePersistentTableInsertStrategy( (PersistentTableInsertStrategy) insertStrategy );
		}
		if ( insertStrategy instanceof GlobalTemporaryTableInsertStrategy ) {
			return new ReactiveGlobalTemporaryTableInsertStrategy( (GlobalTemporaryTableStrategy) insertStrategy );
		}
		return insertStrategy;
	}

	private static SqmMultiTableInsertStrategy insertStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext creationContext) {
		final SessionFactoryOptions options = creationContext.getSessionFactoryOptions();
		return options.getCustomSqmMultiTableInsertStrategy() != null
				? options.getCustomSqmMultiTableInsertStrategy()
				: creationContext.getDialect().getFallbackSqmInsertStrategy( rootEntityDescriptor, creationContext );
	}
}

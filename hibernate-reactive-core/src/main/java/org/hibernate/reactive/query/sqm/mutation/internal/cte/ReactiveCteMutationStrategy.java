/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import java.util.concurrent.CompletionStage;

import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.cte.CteMutationStrategy;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;

public class ReactiveCteMutationStrategy extends CteMutationStrategy implements ReactiveSqmMultiTableMutationStrategy {

	public ReactiveCteMutationStrategy(EntityMappingType rootEntityType, RuntimeModelCreationContext runtimeModelCreationContext) {
		this( rootEntityType.getEntityPersister(), runtimeModelCreationContext );
	}

	public ReactiveCteMutationStrategy(EntityPersister rootDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
		super( rootDescriptor, runtimeModelCreationContext );
	}

	@Override
	public CompletionStage<Integer> reactiveExecuteUpdate(
			SqmUpdateStatement<?> sqmUpdateStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		checkMatch( sqmUpdateStatement );
		return new ReactiveCteUpdateHandler( getIdCteTable(), sqmUpdateStatement, domainParameterXref, this, getSessionFactory() )
				.reactiveExecute( context );
	}

	@Override
	public CompletionStage<Integer> reactiveExecuteDelete(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		checkMatch( sqmDeleteStatement );
		return new ReactiveCteDeleteHandler( getIdCteTable(), sqmDeleteStatement, domainParameterXref, this, getSessionFactory() )
				.reactiveExecute( context );
	}
}

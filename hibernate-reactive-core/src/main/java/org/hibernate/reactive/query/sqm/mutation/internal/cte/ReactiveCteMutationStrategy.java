/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import org.hibernate.internal.util.MutableObject;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.cte.CteMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandler;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.tree.SqmDeleteOrUpdateStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class ReactiveCteMutationStrategy extends CteMutationStrategy implements ReactiveSqmMultiTableMutationStrategy {

	public ReactiveCteMutationStrategy(EntityMappingType rootEntityType, RuntimeModelCreationContext runtimeModelCreationContext) {
		this( rootEntityType.getEntityPersister(), runtimeModelCreationContext );
	}

	public ReactiveCteMutationStrategy(EntityPersister rootDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
		super( rootDescriptor, runtimeModelCreationContext );
	}

	@Override
	public MultiTableHandlerBuildResult buildHandler(SqmDeleteOrUpdateStatement<?> sqmStatement, DomainParameterXref domainParameterXref, DomainQueryExecutionContext context) {
		final MutableObject<JdbcParameterBindings> firstJdbcParameterBindings = new MutableObject<>();
		final MultiTableHandler multiTableHandler = sqmStatement instanceof SqmDeleteStatement<?> sqmDelete
				? buildHandler( sqmDelete, domainParameterXref, context, firstJdbcParameterBindings)
				: buildHandler( (SqmUpdateStatement<?>) sqmStatement, domainParameterXref, context, firstJdbcParameterBindings );
		return new MultiTableHandlerBuildResult( multiTableHandler, firstJdbcParameterBindings.get() );
	}

	public ReactiveHandler buildHandler(SqmDeleteStatement<?> sqmDelete, DomainParameterXref domainParameterXref, DomainQueryExecutionContext context, MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		checkMatch( sqmDelete );
		if ( getRootDescriptor().getSoftDeleteMapping() != null ) {
			return new ReactiveCteSoftDeleteHandler(
					getIdCteTable(),
					sqmDelete,
					domainParameterXref,
					this,
					getSessionFactory(),
					context,
					firstJdbcParameterBindingsConsumer
			);
		}
		else {
			return new ReactiveCteDeleteHandler(
					getIdCteTable(),
					sqmDelete,
					domainParameterXref,
					this,
					getSessionFactory(),
					context,
					firstJdbcParameterBindingsConsumer
			);
		}
	}

	public MultiTableHandler buildHandler(SqmUpdateStatement<?> sqmUpdate, DomainParameterXref domainParameterXref, DomainQueryExecutionContext context, MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		checkMatch( sqmUpdate );
		return new ReactiveCteUpdateHandler(
				getIdCteTable(),
				sqmUpdate,
				domainParameterXref,
				this,
				getSessionFactory(),
				context,
				firstJdbcParameterBindingsConsumer
		);
	}

}

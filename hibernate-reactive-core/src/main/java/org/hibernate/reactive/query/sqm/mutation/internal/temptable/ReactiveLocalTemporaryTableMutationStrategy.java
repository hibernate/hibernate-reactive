/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import org.hibernate.internal.util.MutableObject;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandler;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class ReactiveLocalTemporaryTableMutationStrategy extends LocalTemporaryTableMutationStrategy
		implements ReactiveSqmMultiTableMutationStrategy {

	public ReactiveLocalTemporaryTableMutationStrategy(LocalTemporaryTableMutationStrategy mutationStrategy) {
		super( mutationStrategy.getTemporaryTable(), mutationStrategy.getSessionFactory() );
	}

	@Override
	public MultiTableHandler buildHandler(
			SqmUpdateStatement<?> sqmUpdate,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		return new ReactiveTableBasedUpdateHandler(
				sqmUpdate,
				domainParameterXref,
				getTemporaryTable(),
				getTemporaryTableStrategy(),
				isDropIdTables(),
				session -> {
					throw new UnsupportedOperationException( "Unexpected call to access Session uid" );
				},
				context,
				firstJdbcParameterBindingsConsumer
		);
	}

	@Override
	public MultiTableHandler buildHandler(
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		final EntityPersister rootDescriptor = context.getSession().getFactory().getMappingMetamodel()
				.getEntityDescriptor( sqmDelete.getRoot().getEntityName() );
		if ( rootDescriptor.getSoftDeleteMapping() != null ) {
			return new ReactiveTableBasedSoftDeleteHandler(
					sqmDelete,
					domainParameterXref,
					getTemporaryTable(),
					getTemporaryTableStrategy(),
					isDropIdTables(),
					session -> {
						throw new UnsupportedOperationException( "Unexpected call to access Session uid" );
					},
					context,
					firstJdbcParameterBindingsConsumer
			);
		}
		else {
			return new ReactiveTableBasedDeleteHandler(
					sqmDelete,
					domainParameterXref,
					getTemporaryTable(),
					getTemporaryTableStrategy(),
					isDropIdTables(),
					session -> {
						throw new UnsupportedOperationException( "Unexpected call to access Session uid" );
					},
					context,
					firstJdbcParameterBindingsConsumer
			);
		}
	}
}

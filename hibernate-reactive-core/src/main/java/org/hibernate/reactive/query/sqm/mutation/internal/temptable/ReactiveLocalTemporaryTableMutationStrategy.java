/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.AfterUseAction;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;

public class ReactiveLocalTemporaryTableMutationStrategy extends LocalTemporaryTableMutationStrategy
		implements ReactiveSqmMultiTableMutationStrategy {

	public ReactiveLocalTemporaryTableMutationStrategy(LocalTemporaryTableMutationStrategy mutationStrategy) {
		super( mutationStrategy.getTemporaryTable(), mutationStrategy.getSessionFactory() );
	}

	private static String throwUnexpectedAccessToSessionUID(SharedSessionContractImplementor session) {
		// Should probably go in the LOG
		throw new UnsupportedOperationException( "Unexpected call to access Session uid" );
	}

	@Override
	public InternalStage<Integer> reactiveExecuteUpdate(
			SqmUpdateStatement<?> sqmUpdate,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		return new ReactiveTableBasedUpdateHandler(
				sqmUpdate,
				domainParameterXref,
				getTemporaryTable(),
				afterUseAction(),
				ReactiveLocalTemporaryTableMutationStrategy::throwUnexpectedAccessToSessionUID,
				getSessionFactory()
		).reactiveExecute( context );
	}

	@Override
	public InternalStage<Integer> reactiveExecuteDelete(
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		return new ReactiveTableBasedDeleteHandler(
				sqmDelete,
				domainParameterXref,
				getTemporaryTable(),
				afterUseAction(),
				ReactiveLocalTemporaryTableMutationStrategy::throwUnexpectedAccessToSessionUID,
				getSessionFactory()
		).reactiveExecute( context );
	}

	private AfterUseAction afterUseAction() {
		return isDropIdTables()
				? AfterUseAction.DROP
				: getSessionFactory().getJdbcServices().getDialect().getTemporaryTableAfterUseAction();
	}
}

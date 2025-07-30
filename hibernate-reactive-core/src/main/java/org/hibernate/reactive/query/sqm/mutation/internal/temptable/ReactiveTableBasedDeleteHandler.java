/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedDeleteHandler;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveAbstractMutationHandler;

public class ReactiveTableBasedDeleteHandler extends TableBasedDeleteHandler implements ReactiveAbstractMutationHandler {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public interface ReactiveExecutionDelegate extends TableBasedDeleteHandler.ExecutionDelegate {

		@Override
		default int execute(DomainQueryExecutionContext executionContext) {
			throw LOG.nonReactiveMethodCall( "reactiveExecute" );
		}

		CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext);
	}

	public ReactiveTableBasedDeleteHandler(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			TemporaryTable idTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			SessionFactoryImplementor sessionFactory) {
		super(
				sqmDeleteStatement,
				domainParameterXref,
				idTable,
				temporaryTableStrategy,
				forceDropAfterUse,
				sessionUidAccess,
				sessionFactory
		);
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef( "Starting multi-table delete execution - %s", getSqmDeleteOrUpdateStatement().getRoot().getModel().getName() );
		}
		return resolveDelegate( executionContext ).reactiveExecute( executionContext );
	}

	protected ReactiveExecutionDelegate resolveDelegate(DomainQueryExecutionContext executionContext) {
		if ( getEntityDescriptor().getSoftDeleteMapping() != null ) {
			return new ReactiveSoftDeleteExecutionDelegate(
					getEntityDescriptor(),
					getIdTable(),
					getTemporaryTableStrategy(),
					isForceDropAfterUse(),
					getSqmDeleteOrUpdateStatement(),
					getDomainParameterXref(),
					executionContext.getQueryOptions(),
					executionContext.getSession().getLoadQueryInfluencers(),
					executionContext.getQueryParameterBindings(),
					getSessionUidAccess(),
					getSessionFactory()
			);
		}
		return new ReactiveRestrictedDeleteExecutionDelegate(
				getEntityDescriptor(),
				getIdTable(),
				getTemporaryTableStrategy(),
				isForceDropAfterUse(),
				getSqmDeleteOrUpdateStatement(),
				getDomainParameterXref(),
				executionContext.getQueryOptions(),
				executionContext.getSession().getLoadQueryInfluencers(),
				executionContext.getQueryParameterBindings(),
				getSessionUidAccess(),
				getSessionFactory()
		);
	}
}

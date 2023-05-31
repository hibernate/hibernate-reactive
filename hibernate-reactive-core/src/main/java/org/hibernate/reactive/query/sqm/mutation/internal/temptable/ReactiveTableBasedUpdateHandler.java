/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.internal.temptable.AfterUseAction;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedUpdateHandler;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveAbstractMutationHandler;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.exec.spi.ExecutionContext;

public class ReactiveTableBasedUpdateHandler extends TableBasedUpdateHandler implements ReactiveAbstractMutationHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public interface ReactiveExecutionDelegate extends TableBasedUpdateHandler.ExecutionDelegate {
		@Override
		default int execute(ExecutionContext executionContext) {
			throw LOG.nonReactiveMethodCall( "reactiveExecute" );
		}

		CompletionStage<Integer> reactiveExecute(ExecutionContext executionContext);
	}

	public ReactiveTableBasedUpdateHandler(
			SqmUpdateStatement<?> sqmUpdate,
			DomainParameterXref domainParameterXref,
			TemporaryTable idTable,
			AfterUseAction afterUseAction,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			SessionFactoryImplementor sessionFactory) {
		super( sqmUpdate, domainParameterXref, idTable, afterUseAction, sessionUidAccess, sessionFactory );
	}

	@Override
	public int execute(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Starting multi-table update execution - %s",
					getSqmDeleteOrUpdateStatement().getRoot().getModel().getName()
			);
		}

		final SqmJdbcExecutionContextAdapter executionContextAdapter = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( executionContext );
		return resolveDelegate( executionContext ).reactiveExecute( executionContextAdapter );
	}

	@Override
	protected ReactiveExecutionDelegate resolveDelegate(DomainQueryExecutionContext executionContext) {
		return (ReactiveExecutionDelegate) super.resolveDelegate( executionContext );
	}

	@Override
	protected ReactiveUpdateExecutionDelegate buildExecutionDelegate(
			MultiTableSqmMutationConverter sqmConverter,
			TemporaryTable idTable,
			AfterUseAction afterUseAction,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainParameterXref domainParameterXref,
			TableGroup updatingTableGroup,
			Map<String, TableReference> tableReferenceByAlias,
			List<Assignment> assignments,
			Predicate suppliedPredicate,
			Map<SqmParameter<?>, List<List<JdbcParameter>>> parameterResolutions,
			Map<SqmParameter<?>, MappingModelExpressible<?>> paramTypeResolutions,
			DomainQueryExecutionContext executionContext) {
		return new ReactiveUpdateExecutionDelegate(
				sqmConverter,
				idTable,
				afterUseAction,
				sessionUidAccess,
				domainParameterXref,
				updatingTableGroup,
				tableReferenceByAlias,
				assignments,
				suppliedPredicate,
				parameterResolutions,
				paramTypeResolutions,
				executionContext
		);
	}
}

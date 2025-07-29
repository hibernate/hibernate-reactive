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
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedInsertHandler;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveInsertExecutionDelegate;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.insert.ConflictClause;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.exec.spi.ExecutionContext;

public class ReactiveTableBasedInsertHandler extends TableBasedInsertHandler implements ReactiveHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public interface ReactiveExecutionDelegate extends ExecutionDelegate {
		@Override
		default int execute(ExecutionContext executionContext) {
			throw LOG.nonReactiveMethodCall( "reactiveExecute" );
		}

		CompletionStage<Integer> reactiveExecute(ExecutionContext executionContext);
	}

	public ReactiveTableBasedInsertHandler(
			SqmInsertStatement<?> sqmInsert,
			DomainParameterXref domainParameterXref,
			TemporaryTable entityTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			SessionFactoryImplementor sessionFactory) {
		super( sqmInsert, domainParameterXref, entityTable, temporaryTableStrategy, forceDropAfterUse, sessionUidAccess, sessionFactory );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Starting multi-table insert execution - %s",
					getSqmInsertStatement().getTarget().getModel().getName()
			);
		}

		final SqmJdbcExecutionContextAdapter executionContextAdapter = SqmJdbcExecutionContextAdapter
				.omittingLockingAndPaging( executionContext );
		return resolveDelegate( executionContext )
				.reactiveExecute( executionContextAdapter );
	}

	@Override
	protected ReactiveExecutionDelegate resolveDelegate(DomainQueryExecutionContext executionContext) {
		return (ReactiveExecutionDelegate) super.resolveDelegate( executionContext );
	}

	@Override
	protected ExecutionDelegate buildExecutionDelegate(
			SqmInsertStatement<?> sqmInsert,
			MultiTableSqmMutationConverter sqmConverter,
			TemporaryTable entityTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainParameterXref domainParameterXref,
			TableGroup insertingTableGroup,
			Map<String, TableReference> tableReferenceByAlias,
			List<Assignment> assignments,
			boolean assignsId,
			InsertSelectStatement insertStatement,
			ConflictClause conflictClause,
			JdbcParameter sessionUidParameter,
			DomainQueryExecutionContext executionContext) {
		return new ReactiveInsertExecutionDelegate(
				sqmConverter,
				entityTable,
				temporaryTableStrategy,
				forceDropAfterUse,
				sessionUidAccess,
				domainParameterXref,
				insertingTableGroup,
				tableReferenceByAlias,
				assignments,
				assignsId,
				insertStatement,
				conflictClause,
				sessionUidParameter,
				executionContext
		);
	}
}

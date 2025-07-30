/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.internal.temptable.InsertExecutionDelegate;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveTableBasedInsertHandler;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.insert.ConflictClause;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.exec.spi.ExecutionContext;

/**
 * @see InsertExecutionDelegate
 */
public class ReactiveInsertExecutionDelegate extends InsertExecutionDelegate implements ReactiveTableBasedInsertHandler.ReactiveExecutionDelegate {

	public ReactiveInsertExecutionDelegate(
			MultiTableSqmMutationConverter sqmConverter,
			TemporaryTable entityTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainParameterXref domainParameterXref,
			TableGroup insertingTableGroup,
			Map<String, TableReference> tableReferenceByAlias,
			List<Assignment> assignments,
			boolean assignedId,
			InsertSelectStatement insertStatement,
			ConflictClause conflictClause,
			JdbcParameter sessionUidParameter,
			DomainQueryExecutionContext executionContext) {
		super(
				sqmConverter,
				entityTable,
				temporaryTableStrategy,
				forceDropAfterUse,
				sessionUidAccess,
				domainParameterXref,
				insertingTableGroup,
				tableReferenceByAlias,
				assignments,
				assignedId,
				insertStatement,
				conflictClause,
				sessionUidParameter,
				executionContext
		);
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(ExecutionContext executionContext) {
		// FIXME: Why is this null?
		return null;
	}
}

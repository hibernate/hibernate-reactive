/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.cte.CteInsertHandler;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.cte.CteTable;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

public class ReactiveCteInsertHandler extends CteInsertHandler implements ReactiveHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveCteInsertHandler(
			CteTable cteTable,
			SqmInsertStatement<?> sqmStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super( cteTable, sqmStatement, domainParameterXref, context, firstJdbcParameterBindingsConsumer );
	}

	@Override
	public int execute(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(
			JdbcParameterBindings jdbcParameterBindings,
			DomainQueryExecutionContext context) {
		return ( (ReactiveSharedSessionContractImplementor) context.getSession() )
				.reactiveAutoFlushIfRequired( getSelect().getAffectedTableNames() )
				.thenCompose( v -> StandardReactiveSelectExecutor.INSTANCE
						.list(
								getSelect(),
								jdbcParameterBindings,
								SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context ),
								row -> row[0],
								null,
								ReactiveListResultsConsumer.UniqueSemantic.NONE,
								1
						)
						.thenApply( list -> ( (Number) list.get( 0 ) ).intValue() )
				);
	}
}

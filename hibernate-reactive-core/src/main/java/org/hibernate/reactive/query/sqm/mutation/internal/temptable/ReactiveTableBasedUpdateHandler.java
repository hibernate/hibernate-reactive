/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.AssertionFailure;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedUpdateHandler;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveAbstractMutationHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveTableBasedUpdateHandler extends TableBasedUpdateHandler implements ReactiveAbstractMutationHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveTableBasedUpdateHandler(
			SqmUpdateStatement<?> sqmUpdate,
			DomainParameterXref domainParameterXref,
			TemporaryTable idTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super( sqmUpdate, domainParameterXref, idTable, temporaryTableStrategy, forceDropAfterUse, sessionUidAccess, context, firstJdbcParameterBindingsConsumer );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(JdbcParameterBindings jdbcParameterBindings, DomainQueryExecutionContext context) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Starting multi-table update execution - %s",
					getSqmStatement().getTarget().getModel().getName()
			);
		}

		final SqmJdbcExecutionContextAdapter executionContext = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context );
		return ReactiveExecuteWithTemporaryTableHelper.performBeforeTemporaryTableUseActions(
				getIdTable(),
				getTemporaryTableStrategy(),
				executionContext
		).thenCompose( unused -> ReactiveExecuteWithTemporaryTableHelper
				.saveIntoTemporaryTable(
					getMatchingIdsIntoIdTableInsert().jdbcOperation(),
					jdbcParameterBindings,
					executionContext
			).thenCompose( rows -> loop(getTableUpdaters(), tableUpdater ->
						updateTable( tableUpdater, rows, jdbcParameterBindings, executionContext ) )
						.thenApply(v -> rows))
				.handle( CompletionStages::handle)
				.thenCompose( handler -> ReactiveExecuteWithTemporaryTableHelper
						.performAfterTemporaryTableUseActions( getIdTable(), getSessionUidAccess(), getAfterUseAction(), executionContext )
						.thenCompose( v -> handler.getResultAsCompletionStage() ))
		 );
	}

	private CompletionStage<Void> updateTable(
			TableUpdater tableUpdater,
			int expectedUpdateCount,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		if ( tableUpdater == null ) {
			// no assignments for this table - skip it
			return voidFuture();
		}
		return executeMutation( tableUpdater.jdbcUpdate(), jdbcParameterBindings, executionContext )
				.thenCompose( updateCount -> {
					// We are done when the update count matches
					if ( updateCount == expectedUpdateCount ) {
						return voidFuture();
					}

					// If the table is optional, execute an insert
					if ( tableUpdater.jdbcInsert() != null ) {
						return executeMutation( tableUpdater.jdbcInsert(), jdbcParameterBindings, executionContext )
								.thenAccept( insertCount -> {
									if(insertCount + updateCount != expectedUpdateCount){
										throw new AssertionFailure( "insertCount + updateCount != expectedUpdateCount");
									}
								} );
					}
					return voidFuture();
				} );
	}

	private CompletionStage<Integer> executeMutation(JdbcOperationQueryMutation jdbcUpdate, JdbcParameterBindings jdbcParameterBindings, ExecutionContext executionContext) {
		return StandardReactiveJdbcMutationExecutor.INSTANCE.executeReactive(
				jdbcUpdate,
				jdbcParameterBindings,
				sql -> executionContext.getSession()
						.getJdbcCoordinator()
						.getStatementPreparer()
						.prepareStatement( sql ),
				(integer, preparedStatement) -> {
				},
				executionContext
		);
	}

}

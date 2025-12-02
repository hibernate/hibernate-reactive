/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.CacheableSqmInterpretation;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedSoftDeleteHandler;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import java.lang.invoke.MethodHandles;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class ReactiveTableBasedSoftDeleteHandler extends TableBasedSoftDeleteHandler implements ReactiveHandler {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveTableBasedSoftDeleteHandler(
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref,
			TemporaryTable idTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super(
				sqmDelete,
				domainParameterXref,
				idTable,
				temporaryTableStrategy,
				forceDropAfterUse,
				sessionUidAccess,
				context,
				firstJdbcParameterBindingsConsumer
		);
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(
			JdbcParameterBindings jdbcParameterBindings,
			DomainQueryExecutionContext context) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Starting multi-table delete execution - %s",
					getSqmStatement().getTarget().getModel().getName()
			);
		}
		final SqmJdbcExecutionContextAdapter executionContext = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context );
		StandardReactiveJdbcMutationExecutor jdbcMutationExecutor  = StandardReactiveJdbcMutationExecutor.INSTANCE;

		final CacheableSqmInterpretation<InsertSelectStatement, JdbcOperationQueryMutation> idTableInsert = getIdTableInsert();
		if ( idTableInsert != null ) {
			return ReactiveExecuteWithTemporaryTableHelper.performBeforeTemporaryTableUseActions(
					getIdTable(),
					getTemporaryTableStrategy(),
					executionContext
			).thenCompose( unused ->
				ReactiveExecuteWithTemporaryTableHelper.saveIntoTemporaryTable(
						idTableInsert.jdbcOperation(),
						jdbcParameterBindings,
						executionContext
				).thenCompose( rows -> {
					final JdbcParameterBindings sessionUidBindings = new JdbcParameterBindingsImpl( 1 );
					final JdbcParameter sessionUidParameter = getSessionUidParameter();
					if ( sessionUidParameter != null ) {
						sessionUidBindings.addBinding(
								sessionUidParameter,
								new JdbcParameterBindingImpl(
										sessionUidParameter.getExpressionType().getSingleJdbcMapping(),
										UUID.fromString( getSessionUidAccess().apply( executionContext.getSession() ) )
								)
						);
					}
					return jdbcMutationExecutor.executeReactive(
							getSoftDelete(),
							sessionUidBindings,
							sql -> executionContext.getSession()
									.getJdbcCoordinator()
									.getStatementPreparer()
									.prepareStatement( sql ),
							(integer, preparedStatement) -> {},
							executionContext
					).thenApply( u -> rows );
				} )
						.handle( CompletionStages::handle)
						.thenCompose( handler -> ReactiveExecuteWithTemporaryTableHelper
								.performAfterTemporaryTableUseActions(
										getIdTable(),
										getSessionUidAccess(),
										getAfterUseAction(),
										executionContext)
								.thenCompose( v -> handler.getResultAsCompletionStage() ) )
			 );
		}
		else {
			return jdbcMutationExecutor.executeReactive(
					getSoftDelete(),
					jdbcParameterBindings,
					sql -> executionContext.getSession()
							.getJdbcCoordinator()
							.getStatementPreparer()
							.prepareStatement( sql ),
					(integer, preparedStatement) -> {},
					executionContext
			);
		}
	}
}

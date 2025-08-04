/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.persister.entity.UnionSubclassEntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.CacheableSqmInterpretation;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedDeleteHandler;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveAbstractMutationHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;

public class ReactiveTableBasedDeleteHandler extends TableBasedDeleteHandler implements ReactiveAbstractMutationHandler {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveTableBasedDeleteHandler(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			TemporaryTable idTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super(
				sqmDeleteStatement,
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
	public CompletionStage<Integer> reactiveExecute(JdbcParameterBindings jdbcParameterBindings, DomainQueryExecutionContext context) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Starting multi-table delete execution - %s",
					getSqmStatement().getTarget().getModel().getName()
			);
		}
		final SqmJdbcExecutionContextAdapter executionContext = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context );
		final CacheableSqmInterpretation<InsertSelectStatement, JdbcOperationQueryMutation> idTableInsert = getIdTableInsert();
		final TemporaryTable idTable = getIdTable();
		final StandardReactiveJdbcMutationExecutor jdbcMutationExecutor = StandardReactiveJdbcMutationExecutor.INSTANCE;
		if ( idTableInsert != null ) {
			return ReactiveExecuteWithTemporaryTableHelper.performBeforeTemporaryTableUseActions(
					idTable,
					getTemporaryTableStrategy(),
					executionContext
			).thenCompose( unused ->
				ReactiveExecuteWithTemporaryTableHelper.saveIntoTemporaryTable( idTableInsert.jdbcOperation(), jdbcParameterBindings, executionContext )
						.thenCompose( rows -> executeDelete( jdbcParameterBindings, rows, executionContext, jdbcMutationExecutor ) )
						.handle( CompletionStages::handle )
						.thenCompose( handler -> ReactiveExecuteWithTemporaryTableHelper
								.performAfterTemporaryTableUseActions( idTable, getSessionUidAccess(), getAfterUseAction(), executionContext )
						.thenCompose( v -> handler.getResultAsCompletionStage() ) )
			);
		}
		else {
			return loop(getCollectionTableDeletes() ,delete ->
					reactiveExecute( jdbcParameterBindings, delete, jdbcMutationExecutor, executionContext )
			).thenCompose( v -> {
				final int[] rows = { 0 };
				return deleteRows( jdbcParameterBindings, jdbcMutationExecutor, executionContext, rows )
						.thenApply( vv -> rows[0] );
			} );
		}
	}

	private CompletionStage<Void> deleteRows(JdbcParameterBindings jdbcParameterBindings, StandardReactiveJdbcMutationExecutor jdbcMutationExecutor, SqmJdbcExecutionContextAdapter executionContext, int[] rows) {
		if ( getEntityDescriptor() instanceof UnionSubclassEntityPersister ) {
			return CompletionStages
					.loop( getDeletes(), delete -> reactiveExecute( jdbcParameterBindings, delete, jdbcMutationExecutor, executionContext )
						.thenApply( tot -> rows[0] += tot )
					);
		}
		else {
			return CompletionStages
					.loop( getDeletes(), delete -> reactiveExecute( jdbcParameterBindings, delete, jdbcMutationExecutor, executionContext )
						.thenApply( tot -> rows[0] = tot )
					);
		}
	}

	private CompletionStage<Integer> executeDelete(
			JdbcParameterBindings jdbcParameterBindings,
			Integer rows,
			SqmJdbcExecutionContextAdapter executionContext,
			StandardReactiveJdbcMutationExecutor jdbcMutationExecutor) {
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
		return loop( getCollectionTableDeletes(), delete ->
						reactiveExecute( jdbcParameterBindings, delete, jdbcMutationExecutor, executionContext )
		).thenApply( v -> rows );
	}

	private static CompletionStage<Integer> reactiveExecute(
			JdbcParameterBindings jdbcParameterBindings,
			JdbcOperationQueryMutation delete,
			StandardReactiveJdbcMutationExecutor jdbcMutationExecutor,
			SqmJdbcExecutionContextAdapter executionContext) {
		return jdbcMutationExecutor.executeReactive(
				delete,
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

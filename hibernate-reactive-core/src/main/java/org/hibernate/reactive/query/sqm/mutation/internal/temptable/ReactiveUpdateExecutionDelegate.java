/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.SelectableConsumer;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.internal.temptable.UpdateExecutionDelegate;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.SqlTuple;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.predicate.ExistsPredicate;
import org.hibernate.sql.ast.tree.predicate.InSubQueryPredicate;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.results.internal.SqlSelectionImpl;

import static org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveExecuteWithTemporaryTableHelper.createIdTableSelectQuerySpec;
import static org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveExecuteWithTemporaryTableHelper.performAfterTemporaryTableUseActions;
import static org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveExecuteWithTemporaryTableHelper.performBeforeTemporaryTableUseActions;
import static org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveExecuteWithTemporaryTableHelper.saveMatchingIdsIntoIdTable;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveUpdateExecutionDelegate extends UpdateExecutionDelegate implements ReactiveTableBasedUpdateHandler.ReactiveExecutionDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveUpdateExecutionDelegate(
			MultiTableSqmMutationConverter sqmConverter,
			TemporaryTable idTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainParameterXref domainParameterXref,
			TableGroup updatingTableGroup,
			Map<String, TableReference> tableReferenceByAlias,
			List<Assignment> assignments,
			Predicate suppliedPredicate,
			DomainQueryExecutionContext executionContext) {
		super(
				sqmConverter,
				idTable,
				temporaryTableStrategy,
				forceDropAfterUse,
				sessionUidAccess,
				domainParameterXref,
				updatingTableGroup,
				tableReferenceByAlias,
				assignments,
				suppliedPredicate,
				executionContext
		);
	}

	private static void doNothing(Integer integer, PreparedStatement preparedStatement) {
	}

	@Override
	public int execute(ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(ExecutionContext executionContext) {
		return performBeforeTemporaryTableUseActions(
						getIdTable(),
						getTemporaryTableStrategy(),
						executionContext
				)
				.thenCompose( v -> saveMatchingIdsIntoIdTable(
						getSqmConverter(),
						getSuppliedPredicate(),
						getIdTable(),
						getSessionUidAccess(),
						getJdbcParameterBindings(),
						executionContext
				) )
				.thenCompose( rows -> {
					final QuerySpec idTableSubQuery = createIdTableSelectQuerySpec(
							getIdTable(),
							getSessionUidAccess(),
							getEntityDescriptor(),
							executionContext
					);

					final CompletionStage<Void>[] resultStage = new CompletionStage[] { voidFuture() };
					getEntityDescriptor().visitConstraintOrderedTables(
							(tableExpression, tableKeyColumnVisitationSupplier) -> resultStage[0] = resultStage[0].thenCompose(
									v -> reactiveUpdateTable(
											tableExpression,
											tableKeyColumnVisitationSupplier,
											rows,
											idTableSubQuery,
											executionContext
									) )
					);
					return resultStage[0].thenApply( v -> rows );
				})
				.handle( CompletionStages::handle )
				.thenCompose( handler -> performAfterTemporaryTableUseActions(
								getIdTable(),
								getSessionUidAccess(),
								getAfterUseAction(),
								executionContext
						)
						.thenCompose( handler::getResultAsCompletionStage )
				);
	}

	private CompletionStage<Void> reactiveUpdateTable(
			String tableExpression,
			Supplier<Consumer<SelectableConsumer>> tableKeyColumnVisitationSupplier,
			int expectedUpdateCount,
			QuerySpec idTableSubQuery,
			ExecutionContext executionContext) {

		// update `updatingTableReference`
		// set ...
		// where `keyExpression` in ( `idTableSubQuery` )

		final TableReference updatingTableReference = getUpdatingTableGroup().getTableReference(
				getUpdatingTableGroup().getNavigablePath(),
				tableExpression,
				true
		);

		final List<Assignment> assignments = getAssignmentsByTable().get( updatingTableReference );
		if ( assignments == null || assignments.isEmpty() ) {
			// no assignments for this table - skip it
			return voidFuture();
		}

		final NamedTableReference dmlTableReference = resolveUnionTableReference( updatingTableReference, tableExpression );
		final JdbcServices jdbcServices = getSessionFactory().getJdbcServices();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcServices.getJdbcEnvironment().getSqlAstTranslatorFactory();

		final Expression keyExpression = resolveMutatingTableKeyExpression( tableExpression, tableKeyColumnVisitationSupplier );

		return executeUpdate( idTableSubQuery, executionContext, assignments, dmlTableReference, sqlAstTranslatorFactory, keyExpression )
				.thenCompose( updateCount -> {
					// We are done when the update count matches
					if ( updateCount == expectedUpdateCount ) {
						return voidFuture();
					}
					// If the table is optional, execute an insert
					if ( isTableOptional( tableExpression ) ) {
						return executeInsert(
								tableExpression,
								dmlTableReference,
								keyExpression,
								tableKeyColumnVisitationSupplier,
								idTableSubQuery,
								assignments,
								sqlAstTranslatorFactory,
								executionContext
						)
								.thenAccept( insertCount -> {
									assert insertCount + updateCount == expectedUpdateCount;
								} );
					}
					return voidFuture();
				} );
	}


	private CompletionStage<Integer> executeUpdate(QuerySpec idTableSubQuery, ExecutionContext executionContext, List<Assignment> assignments, NamedTableReference dmlTableReference, SqlAstTranslatorFactory sqlAstTranslatorFactory, Expression keyExpression) {
		final UpdateStatement sqlAst = new UpdateStatement(
				dmlTableReference,
				assignments,
				new InSubQueryPredicate( keyExpression, idTableSubQuery, false )
		);

		final JdbcOperationQueryMutation jdbcUpdate = sqlAstTranslatorFactory
				.buildMutationTranslator( getSessionFactory(), sqlAst )
				.translate( getJdbcParameterBindings(), executionContext.getQueryOptions() );

		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcUpdate,
						getJdbcParameterBindings(),
						executionContext.getSession()
						.getJdbcCoordinator()
						.getStatementPreparer()
						::prepareStatement,
						ReactiveUpdateExecutionDelegate::doNothing,
						executionContext
		);
	}

	private CompletionStage<Integer> executeInsert(
			String targetTableExpression,
			NamedTableReference targetTableReference,
			Expression targetTableKeyExpression,
			Supplier<Consumer<SelectableConsumer>> tableKeyColumnVisitationSupplier,
			QuerySpec idTableSubQuery,
			List<Assignment> assignments,
			SqlAstTranslatorFactory sqlAstTranslatorFactory,
			ExecutionContext executionContext) {

		// Execute a query in the form -
		//
		// insert into <target> (...)
		// 		select ...
		// 		from <id-table> temptable_
		// 		where not exists (
		// 			select 1
		//			from <target> dml_
		//			where dml_.<key> = temptable_.<key>
		// 		)

		// Create a new QuerySpec for the "insert source" select query.  This
		// is mostly a copy of the incoming `idTableSubQuery` along with the
		// NOT-EXISTS predicate
		final QuerySpec insertSourceSelectQuerySpec = makeInsertSourceSelectQuerySpec( idTableSubQuery );

		// create the `select 1 ...` sub-query and apply the not-exists predicate
		final QuerySpec existsSubQuerySpec = createExistsSubQuerySpec( targetTableExpression, tableKeyColumnVisitationSupplier, idTableSubQuery );
		insertSourceSelectQuerySpec.applyPredicate(
				new ExistsPredicate(
						existsSubQuerySpec,
						true,
						getSessionFactory().getTypeConfiguration().getBasicTypeForJavaType( Boolean.class )
				)
		);

		// Collect the target column references from the key expressions
		final List<ColumnReference> targetColumnReferences = new ArrayList<>();
		if ( targetTableKeyExpression instanceof SqlTuple ) {
			//noinspection unchecked
			targetColumnReferences.addAll( (Collection<? extends ColumnReference>) ( (SqlTuple) targetTableKeyExpression ).getExpressions() );
		}
		else {
			targetColumnReferences.add( (ColumnReference) targetTableKeyExpression );
		}

		// And transform assignments to target column references and selections
		for ( Assignment assignment : assignments ) {
			targetColumnReferences.addAll( assignment.getAssignable().getColumnReferences() );
			insertSourceSelectQuerySpec.getSelectClause()
					.addSqlSelection( new SqlSelectionImpl( assignment.getAssignedValue() ) );
		}

		final InsertSelectStatement insertSqlAst = new InsertSelectStatement( targetTableReference );
		insertSqlAst.addTargetColumnReferences( targetColumnReferences.toArray( new ColumnReference[0] ) );
		insertSqlAst.setSourceSelectStatement( insertSourceSelectQuerySpec );

		final JdbcOperationQueryMutation jdbcInsert = sqlAstTranslatorFactory
				.buildMutationTranslator( getSessionFactory(), insertSqlAst )
				.translate( getJdbcParameterBindings(), executionContext.getQueryOptions() );

		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcInsert,
						getJdbcParameterBindings(),
						executionContext.getSession()
								.getJdbcCoordinator()
								.getStatementPreparer()
								::prepareStatement,
						ReactiveUpdateExecutionDelegate::doNothing,
						executionContext
				);
	}
}

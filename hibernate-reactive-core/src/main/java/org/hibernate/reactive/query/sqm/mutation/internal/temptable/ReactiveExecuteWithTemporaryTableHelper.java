/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.boot.TempTableDdlTransactionHandling;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableColumn;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.BasicValuedMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.spi.AfterUseAction;
import org.hibernate.query.sqm.mutation.spi.BeforeUseAction;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveTemporaryTableHelper.TemporaryTableCreationWork;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstJoinType;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.QueryLiteral;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.StandardTableGroup;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.predicate.ComparisonPredicate;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.internal.SqlSelectionImpl;

import static org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveTemporaryTableHelper.cleanTemporaryTableRows;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.query.sqm.mutation.internal.temptable.ExecuteWithTemporaryTableHelper
 */
public final class ReactiveExecuteWithTemporaryTableHelper {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private ReactiveExecuteWithTemporaryTableHelper() {
	}

	public static CompletionStage<Integer> saveMatchingIdsIntoIdTable(
			MultiTableSqmMutationConverter sqmConverter,
			Predicate suppliedPredicate,
			TemporaryTable idTable,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {

		final TableGroup mutatingTableGroup = sqmConverter.getMutatingTableGroup();

		assert mutatingTableGroup.getModelPart() instanceof EntityMappingType;
		final EntityMappingType mutatingEntityDescriptor = (EntityMappingType) mutatingTableGroup.getModelPart();

		final NamedTableReference idTableReference = new NamedTableReference(
				idTable.getTableExpression(),
				InsertSelectStatement.DEFAULT_ALIAS
		);
		final InsertSelectStatement idTableInsert = new InsertSelectStatement( idTableReference );

		for ( int i = 0; i < idTable.getColumns().size(); i++ ) {
			final TemporaryTableColumn column = idTable.getColumns().get( i );
			idTableInsert.addTargetColumnReferences(
					new ColumnReference(
							idTableReference,
							column.getColumnName(),
							// id columns cannot be formulas and cannot have custom read and write expressions
							false,
							null,
							column.getJdbcMapping()
					)
			);
		}

		final QuerySpec matchingIdSelection = new QuerySpec( true, 1 );
		idTableInsert.setSourceSelectStatement( matchingIdSelection );

		matchingIdSelection.getFromClause().addRoot( mutatingTableGroup );

		mutatingEntityDescriptor.getIdentifierMapping().forEachSelectable(
				(selectionIndex, selection) -> {
					matchingIdSelection.getSelectClause().addSqlSelection(
							new SqlSelectionImpl(
									selectionIndex + 1,
									sqmConverter.getSqlExpressionResolver().resolveSqlExpression(
											mutatingTableGroup.resolveTableReference( mutatingTableGroup.getNavigablePath(), selection.getContainingTableExpression() ),
											selection
									)
							)
					);
				}
		);

		if ( idTable.getSessionUidColumn() != null ) {
			final int jdbcPosition = matchingIdSelection.getSelectClause().getSqlSelections().size();
			matchingIdSelection.getSelectClause().addSqlSelection(
					new SqlSelectionImpl(
							jdbcPosition,
							new QueryLiteral<>(
									UUID.fromString( sessionUidAccess.apply( executionContext.getSession() ) ),
									(BasicValuedMapping) idTable.getSessionUidColumn().getJdbcMapping()
							)
					)
			);
		}

		matchingIdSelection.applyPredicate( suppliedPredicate );
		return saveIntoTemporaryTable( idTableInsert, jdbcParameterBindings, executionContext );
	}

	public static CompletionStage<Integer> saveIntoTemporaryTable(
			InsertSelectStatement temporaryTableInsert,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final JdbcServices jdbcServices = factory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();
		final LockOptions lockOptions = executionContext.getQueryOptions().getLockOptions();
		final LockMode lockMode = lockOptions.getLockMode();
		// Acquire a WRITE lock for the rows that are about to be modified
		lockOptions.setLockMode( LockMode.WRITE );
		// Visit the table joins and reset the lock mode if we encounter OUTER joins that are not supported
		if ( temporaryTableInsert.getSourceSelectStatement() != null
				&& !jdbcEnvironment.getDialect().supportsOuterJoinForUpdate() ) {
			temporaryTableInsert.getSourceSelectStatement().visitQuerySpecs(
					querySpec -> querySpec.getFromClause().visitTableJoins(
								tableJoin -> {
									if ( tableJoin.isInitialized()
											&& tableJoin.getJoinType() != SqlAstJoinType.INNER ) {
										lockOptions.setLockMode( lockMode );
									}
								}
						)
			);
		}
		final JdbcOperationQueryMutation jdbcInsert = sqlAstTranslatorFactory.buildMutationTranslator( factory, temporaryTableInsert )
				.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		lockOptions.setLockMode( lockMode );

		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcInsert,
						jdbcParameterBindings,
						executionContext.getSession().getJdbcCoordinator().getStatementPreparer()::prepareStatement,
						ReactiveExecuteWithTemporaryTableHelper::doNothing,
						executionContext

				);
	}

	public static QuerySpec createIdTableSelectQuerySpec(
			TemporaryTable idTable,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			EntityMappingType entityDescriptor,
			ExecutionContext executionContext) {
		return createIdTableSelectQuerySpec( idTable, null, sessionUidAccess, entityDescriptor, executionContext );
	}

	public static QuerySpec createIdTableSelectQuerySpec(
			TemporaryTable idTable,
			ModelPart fkModelPart,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			EntityMappingType entityDescriptor,
			ExecutionContext executionContext) {
		final QuerySpec querySpec = new QuerySpec( false );

		final NamedTableReference idTableReference = new NamedTableReference(
				idTable.getTableExpression(),
				TemporaryTable.DEFAULT_ALIAS,
				true
		);
		final TableGroup idTableGroup = new StandardTableGroup(
				true,
				new NavigablePath( idTableReference.getTableExpression() ),
				entityDescriptor,
				null,
				idTableReference,
				null,
				executionContext.getSession().getFactory()
		);

		querySpec.getFromClause().addRoot( idTableGroup );

		applyIdTableSelections( querySpec, idTableReference, idTable, fkModelPart, entityDescriptor );
		applyIdTableRestrictions( querySpec, idTableReference, idTable, sessionUidAccess, executionContext );

		return querySpec;
	}

	// TODO: I think we can reuse the method in ExecuteWithTemporaryTableHelper
	private static void applyIdTableSelections(
			QuerySpec querySpec,
			TableReference tableReference,
			TemporaryTable idTable,
			ModelPart fkModelPart,
			EntityMappingType entityDescriptor) {
		if ( fkModelPart == null ) {
			final int size = entityDescriptor.getIdentifierMapping().getJdbcTypeCount();
			for ( int i = 0; i < size; i++ ) {
				final TemporaryTableColumn temporaryTableColumn = idTable.getColumns().get( i );
				if ( temporaryTableColumn != idTable.getSessionUidColumn() ) {
					querySpec.getSelectClause().addSqlSelection(
							new SqlSelectionImpl(
									i,
									new ColumnReference(
											tableReference,
											temporaryTableColumn.getColumnName(),
											false,
											null,
											temporaryTableColumn.getJdbcMapping()
									)
							)
					);
				}
			}
		}
		else {
			fkModelPart.forEachSelectable(
					(i, selectableMapping) -> querySpec.getSelectClause()
							.addSqlSelection( new SqlSelectionImpl(
									i,
									new ColumnReference(
											tableReference,
											selectableMapping.getSelectionExpression(),
											false,
											null,
											selectableMapping.getJdbcMapping()
									)
							)
					)
			);
		}
	}

	private static void applyIdTableRestrictions(
			QuerySpec querySpec,
			TableReference idTableReference,
			TemporaryTable idTable,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			ExecutionContext executionContext) {
		if ( idTable.getSessionUidColumn() != null ) {
			querySpec.applyPredicate(
					new ComparisonPredicate(
							new ColumnReference(
									idTableReference,
									idTable.getSessionUidColumn().getColumnName(),
									false, null,
									idTable.getSessionUidColumn().getJdbcMapping()
							),
							ComparisonOperator.EQUAL,
							new QueryLiteral<>(
									UUID.fromString( sessionUidAccess.apply( executionContext.getSession() ) ),
									(BasicValuedMapping) idTable.getSessionUidColumn().getJdbcMapping()
							)
					)
			);
		}
	}

	@Deprecated(forRemoval = true, since = "3.1")
	public static CompletionStage<Void> performBeforeTemporaryTableUseActions(
			TemporaryTable temporaryTable,
			ExecutionContext executionContext) {
		return performBeforeTemporaryTableUseActions(
				temporaryTable,
				executionContext.getSession().getDialect().getTemporaryTableBeforeUseAction(),
				executionContext
		).thenCompose( CompletionStages::voidFuture );
	}

	public static CompletionStage<Boolean> performBeforeTemporaryTableUseActions(
			TemporaryTable temporaryTable,
			TemporaryTableStrategy temporaryTableStrategy,
			ExecutionContext executionContext) {
		return performBeforeTemporaryTableUseActions( temporaryTable, temporaryTableStrategy.getTemporaryTableBeforeUseAction(), executionContext );
	}

	public static CompletionStage<Boolean> performBeforeTemporaryTableUseActions(
			TemporaryTable temporaryTable,
			BeforeUseAction beforeUseAction,
			ExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final Dialect dialect = factory.getJdbcServices().getDialect();
		if ( beforeUseAction == BeforeUseAction.CREATE ) {
			final TemporaryTableCreationWork temporaryTableCreationWork = new TemporaryTableCreationWork( temporaryTable, factory );
			final TempTableDdlTransactionHandling ddlTransactionHandling = dialect.getTemporaryTableDdlTransactionHandling();
			if ( ddlTransactionHandling == TempTableDdlTransactionHandling.NONE ) {
				return temporaryTableCreationWork.reactiveExecute( ( (ReactiveConnectionSupplier) executionContext.getSession() ).getReactiveConnection() );
			}
			throw LOG.notYetImplemented();
		}
		return falseFuture();
	}

	public static CompletionStage<Void> performAfterTemporaryTableUseActions(
			TemporaryTable temporaryTable,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			AfterUseAction afterUseAction,
			ExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final Dialect dialect = factory.getJdbcServices().getDialect();
		return switch ( afterUseAction ) {
			case CLEAN -> cleanTemporaryTableRows( temporaryTable, dialect.getTemporaryTableExporter(), sessionUidAccess, executionContext.getSession() );
			case DROP -> dropAction( temporaryTable, executionContext, factory, dialect );
			default -> voidFuture();
		};
	}

	private static CompletionStage<Void> dropAction(
			TemporaryTable temporaryTable,
			ExecutionContext executionContext,
			SessionFactoryImplementor factory,
			Dialect dialect) {
		final TempTableDdlTransactionHandling ddlTransactionHandling = dialect.getTemporaryTableDdlTransactionHandling();
		if ( ddlTransactionHandling == TempTableDdlTransactionHandling.NONE ) {
			return new ReactiveTemporaryTableHelper
					.TemporaryTableDropWork( temporaryTable, factory )
					.reactiveExecute( ( (ReactiveConnectionSupplier) executionContext.getSession() ).getReactiveConnection() )
					.thenCompose( CompletionStages::voidFuture );
		}

		return failedFuture( LOG.notYetImplemented() );
	}

	private static void doNothing(Integer integer, PreparedStatement preparedStatement) {
	}
}

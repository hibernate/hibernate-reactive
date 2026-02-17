/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.boot.TempTableDdlTransactionHandling;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableSessionUidColumn;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.query.sqm.mutation.internal.temptable.ExecuteWithTemporaryTableHelper;
import org.hibernate.query.sqm.mutation.spi.AfterUseAction;
import org.hibernate.query.sqm.mutation.spi.BeforeUseAction;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactiveTemporaryTableHelper.TemporaryTableCreationWork;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.SqlAstJoinType;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

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
		final QueryPart sourceSelectStatement = temporaryTableInsert.getSourceSelectStatement();
		if ( sourceSelectStatement != null
				&& !jdbcEnvironment.getDialect().supportsOuterJoinForUpdate() ) {
			sourceSelectStatement.visitQuerySpecs(
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

		return saveIntoTemporaryTable(jdbcInsert, jdbcParameterBindings, executionContext);
	}

	public static CompletionStage<Integer> saveIntoTemporaryTable(
			JdbcOperationQueryMutation jdbcInsert,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcInsert,
						jdbcParameterBindings,
						sql -> executionContext.getSession().getJdbcCoordinator()
								.getStatementPreparer().prepareStatement( sql ),
						(integer, preparedStatement) -> {},
						executionContext
				);
	}

	public static QuerySpec createIdTableSelectQuerySpec(
			TemporaryTable idTable,
			JdbcParameter sessionUidParameter,
			EntityMappingType entityDescriptor,
			ExecutionContext executionContext) {
		return createIdTableSelectQuerySpec( idTable, null, sessionUidParameter, entityDescriptor, executionContext );
	}

	public static QuerySpec createIdTableSelectQuerySpec(
			TemporaryTable idTable,
			ModelPart fkModelPart,
			JdbcParameter sessionUidParameter,
			EntityMappingType entityDescriptor,
			ExecutionContext executionContext) {
		return ExecuteWithTemporaryTableHelper.createIdTableSelectQuerySpec( idTable, fkModelPart, sessionUidParameter, entityDescriptor, executionContext );
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

	public static CompletionStage<Integer[]> loadInsertedRowNumbers(
			String sqlSelect,
			TemporaryTable temporaryTable,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			int rows,
			ExecutionContext executionContext) {
		final TemporaryTableSessionUidColumn sessionUidColumn = temporaryTable.getSessionUidColumn();
		final SharedSessionContractImplementor session = executionContext.getSession();
		Object[] parameters = new Object[1];
		if ( sessionUidColumn != null ) {
			parameters[0] = UUID.fromString( sessionUidAccess.apply( session ) );
		}
		final Integer[] rowNumbers = new Integer[rows];
		return reactiveConnection(session).selectJdbc( sqlSelect, parameters )
				.thenApply( resultSet -> getRowNumbers( rows, resultSet, rowNumbers ) );
	}

	private static Integer[] getRowNumbers(int rows, ResultSet resultSet, Integer[] rowNumbers) {
		int rowIndex = 0;
		try {
			while ( resultSet.next() ) {
				rowNumbers[rowIndex++] = resultSet.getInt( 1 );
			}
			return rowNumbers;
		}
		catch ( IndexOutOfBoundsException e ) {
			throw new IllegalArgumentException( "Expected " + rows + " to be inserted but found more", e );
		}
		catch ( SQLException ex ) {
			throw new IllegalStateException( ex );
		}
	}

	private static ReactiveConnection reactiveConnection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
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

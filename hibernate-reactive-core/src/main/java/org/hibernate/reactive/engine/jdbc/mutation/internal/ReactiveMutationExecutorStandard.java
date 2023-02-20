/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.sql.SQLException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.checkResults;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_TRACE_ENABLED;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard
 */
public class ReactiveMutationExecutorStandard extends MutationExecutorStandard implements ReactiveMutationExecutor {

	public ReactiveMutationExecutorStandard(
			MutationOperationGroup mutationOperationGroup,
			Supplier<BatchKey> batchKeySupplier,
			int batchSize,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, batchKeySupplier, batchSize, session );
	}

	private ReactiveConnection connection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
	}

	@Override
	public CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker) {
		return ReactiveMutationExecutor.super.performReactiveBatchedOperations( valuesAnalysis, inclusionChecker );
	}

	@Override
	protected void performNonBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "performReactiveNonBatchedOperations" );
	}

	@Override
	protected void performSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "performReactiveSelfExecutingOperations" );
	}

	@Override
	protected void performBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker) {
		throw LOG.nonReactiveMethodCall( "performReactiveBatchedOperations" );
	}

	@Override
	public CompletionStage<Void> performReactiveNonBatchedMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		if ( statementDetails == null ) {
			return voidFuture();
		}

		final TableMapping tableDetails = statementDetails.getMutatingTableDetails();
		if ( inclusionChecker != null && !inclusionChecker.include( tableDetails ) ) {
			if ( MODEL_MUTATION_LOGGER_TRACE_ENABLED ) {
				MODEL_MUTATION_LOGGER
						.tracef( "Skipping execution of secondary insert : %s", tableDetails.getTableName() );
			}
			return voidFuture();
		}

		// If we get here the statement is needed - make sure it is resolved
		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );
		valueBindings.beforeStatement( statementDetails );

		return connection( session )
				.update( statementDetails.getSqlString() )
				.thenCompose( affectedRowCount -> checkResult( session, statementDetails, resultChecker, tableDetails, affectedRowCount ) )
				.whenComplete( (unused, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}

	private static CompletionStage<Void> checkResult(
			SharedSessionContractImplementor session,
			PreparedStatementDetails statementDetails,
			OperationResultChecker resultChecker,
			TableMapping tableDetails,
			Integer affectedRowCount) {
		if ( affectedRowCount == 0 && tableDetails.isOptional() ) {
			// the optional table did not have a row
			return voidFuture();
		}
		try {
			checkResults( resultChecker, statementDetails, affectedRowCount, -1 );
			return voidFuture();
		}
		catch (SQLException e) {
			Throwable exception = session.getJdbcServices().getSqlExceptionHelper().convert(
					e,
					String.format(
							"Unable to execute mutation PreparedStatement against table `%s`",
							tableDetails.getTableName()
					),
					statementDetails.getSqlString()
			);
			return failedFuture( exception );
		}
	}

	@Override
	public void release() {
	}
}

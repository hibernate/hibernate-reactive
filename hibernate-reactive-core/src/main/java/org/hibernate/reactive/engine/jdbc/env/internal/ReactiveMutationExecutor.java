/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.env.internal;

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_TRACE_ENABLED;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.AbstractMutationExecutor
 */
public interface ReactiveMutationExecutor extends MutationExecutor {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default Object execute(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "executeReactive" );
	}

	default CompletionStage<Object> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return performReactiveNonBatchedOperations( valuesAnalysis, inclusionChecker, resultChecker, session )
				.thenCompose( ignore -> performReactiveSelfExecutingOperations( valuesAnalysis, inclusionChecker, session ) )
				.thenCompose( ignore -> performReactiveBatchedOperations( valuesAnalysis, inclusionChecker ) )
				.thenApply( CompletionStages::nullFuture );
	}

	default CompletionStage<Void> performReactiveNonBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return voidFuture();
	}

	default CompletionStage<Void> performReactiveSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		return voidFuture();
	}

	default CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker) {
		return voidFuture();
	}

	/**
	 * Perform a non-batched mutation
	 */
	default CompletionStage<Void> performReactiveNonBatchedMutation(
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
				MODEL_MUTATION_LOGGER.tracef( "Skipping execution of secondary insert : %s", tableDetails.getTableName() );
			}
			return voidFuture();
		}

		// If we get here the statement is needed - make sure it is resolved
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			valueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params )
				.thenCompose( affectedRowCount -> {
					if ( affectedRowCount == 0 && tableDetails.isOptional() ) {
						// the optional table did not have a row
						return voidFuture();
					}
					checkResults( session, statementDetails, resultChecker, affectedRowCount );
					return voidFuture();
				} )
				.whenComplete( (o, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}

	private static void checkResults(
			SharedSessionContractImplementor session,
			PreparedStatementDetails statementDetails,
			OperationResultChecker resultChecker,
			Integer affectedRowCount) {
		try {
			ModelMutationHelper.checkResults( resultChecker, statementDetails, affectedRowCount, -1 );
		}
		catch (SQLException e) {
			throw session.getJdbcServices().getSqlExceptionHelper()
					.convert(
					e,
					String.format(
							"Unable to execute mutation PreparedStatement against table `%s`",
							statementDetails.getMutatingTableDetails().getTableName()
					),
					statementDetails.getSqlString()
			);
		}
	}
}

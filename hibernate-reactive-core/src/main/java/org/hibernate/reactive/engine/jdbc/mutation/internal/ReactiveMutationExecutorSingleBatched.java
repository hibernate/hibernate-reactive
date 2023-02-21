/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_TRACE_ENABLED;
import java.util.concurrent.CompletionStage;
import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleBatched;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.engine.jdbc.ResultsCheckerUtil;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;

public class ReactiveMutationExecutorSingleBatched extends MutationExecutorSingleBatched implements
		ReactiveMutationExecutor {

	public ReactiveMutationExecutorSingleBatched(
			PreparableMutationOperation mutationOperation,
			BatchKey batchKey,
			int batchSize,
			SharedSessionContractImplementor session) {
		super( mutationOperation, batchKey, batchSize, session );
	}

	@Override
	public CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {

		PreparedStatementDetails statementDetails = getStatementGroup().getSingleStatementDetails();
		JdbcValueBindings valueBindings = getJdbcValueBindings();

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
		Object[] paramValues = PreparedStatementAdaptor.bind(statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			valueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sql = statementDetails.getSqlString();
		return reactiveConnection
				.update(sql, paramValues, true,
						(rowCount, batchPosition, query) -> ResultsCheckerUtil.checkResults(session, statementDetails, resultChecker, rowCount, batchPosition))
				.whenComplete( (o, throwable) -> { //TODO: is this part really needed?
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}
}
